# ============================================================================
# lambda_function.py — RIO-RouteRelaciones (antes RIO-ASP-ROUTE-MT)
# ----------------------------------------------------------------------------
# Ruteo de relaciones entre dispositivos LoRa (2026-10-01): renombrada desde
# RIO-ASP-ROUTE-MT porque pasa a ser la encargada de TODAS las relaciones
# entre nodos RIO que configura GIOMaps (maestro de relaciones). Por ahora
# solo implementa la relacion aspersor -> motor descrita abajo.
#
# DOS CAMINOS (2026-10-01), segun la forma del evento:
#   - DynamoDB Stream de GIOEstadoVivo ({"Records": [...]}): relaciones y
#     parametros desde el espejo de maestros de GIOMaps, lecturas desde el
#     estado en vivo — ver ruteo_relaciones.py. Es el camino nuevo.
#   - Registro plano de la IoT Rule sobre RIO/ASP/DECODED (camino anterior,
#     descrito abajo): RIO-AspersorMotorMap. Se mantiene mientras la regla
#     siga activa; retirarlo junto con la regla y esa tabla.
# ----------------------------------------------------------------------------
# Puente entre los nodos aspersor (RIO-ASP) y los nodos motor (RIO-DSL).
# NO reemplaza ni modifica ninguna de las cuatro Lambdas existentes
# (RIO-ASP-DecodeUplink, RIO-ASP-SendDownlink, RIO-DSL-DecodeUplink,
# RIO-DSL-SendDownlink) — se apoya en ellas tal como están.
#
# ⚠️ ESTADO DEL FIRMWARE DEL MOTOR (actualizado 2026-09-09, ver README DSL.md
# secciones 4.3 y 8): a partir del 2026-09-07 MODO sí rama el ciclo de
# control real (antes era un no-op aceptado/ACKeado sin efecto). En
# MODO=2 (Remoto), PRESION alimenta un lazo real vía la fórmula lineal
# `setpoint = PRESION_OFFSET_RPM + PRESION_GANANCIA_RPM * PRESION`. Para
# que esto mueva el motor DSL-0001 hacen falta, del lado de firmware/AWS
# (no de esta Lambda):
#   1. El nodo tiene que estar en MODO=2 explícito — el default tras el
#      update de firmware es MODO=0 (Ralentí); un nodo que nunca recibió
#      `MODO 2` no reacciona aunque PRESION llegue y se ACKee OK.
#   2. PRESION_GANANCIA_RPM / PRESION_OFFSET_RPM (IDs 30/31) calibrados
#      en campo — vienen en 0 por default, con lo que MODO=2 no hace
#      nada hasta calibrarlos (solo se tocan con CONTROL_HABILITADO=7).
#   3. send_downlink.py (AWS) todavía no tiene entradas en
#      PARAMETER_TABLE para los IDs 30/31 — pendiente agregarlas para
#      poder calibrar por downlink LoRa (hoy solo por serial en banco).
# El watchdog TIMEOUT_SIN_COMANDO_S (ID 11) fuerza ralentí si MODO=2 deja
# de recibir PRESION fresca — esto es justo lo que transporta esta
# Lambda.
#   ⚠️ CORREGIDO 2026-09-09 (señalado por el proyecto DSL, análisis
#   previo de esta Lambda estaba mal planteado): DEBOUNCE_MIN_INTERVAL_
#   SECONDS=60s es un TECHO de tasa (nunca publicamos más seguido que
#   eso si llegan lecturas muy seguidas) — NO es una garantía de que
#   efectivamente publiquemos cada 60s. La cadencia real la determina el
#   aspersor mismo: reporta cada 15-20 min (ver comentario de
#   DEFAULT_TIMEOUT_SIN_COMANDO_S en calibracion_flash.c del firmware:
#   "30 min, mayor al Estado 1 del aspersor, 15-20 min"). Como esta
#   Lambda reacciona a cada uplink, en la práctica publica a DSL-0001
#   cada ~15-20 min (900-1200s), no cada 60s. Contra el
#   DEFAULT_TIMEOUT_SIN_COMANDO_S=1800s del firmware, el margen real es
#   de ~1.5x-2x, no 30x. CONCLUSIÓN: TIMEOUT_SIN_COMANDO_S en DSL-0001
#   debe quedarse en el default (1800s) o más — NO bajarlo cerca de
#   60-120s, o el motor va a caer a ralentí entre reportes normales del
#   aspersor aunque todo funcione bien. Esta Lambda no controla ese
#   parámetro (vive en el firmware/otro proyecto), solo se documenta acá
#   la dependencia.
#
# FLUJO COMPLETO:
#
#   RIO-ASP-DecodeUplink ya publica cada frame de tracker decodificado en
#   RIO/ASP/DECODED, con forma { "codigo_maquinaria": "ASP-0001",
#   "presion": 26, ... }.
#
#   ⚠️ Contrato de entrada VERIFICADO (2026-09-09) contra el código real de
#   RIO-ASP-DecodeUplink, no asumido: build_gio_software_record() en
#   RIO-ASP-DecodeUplink/decoder.py (líneas ~1946-1989) es exactamente lo
#   que se publica en RIO/ASP/DECODED para un frame de tracker, y coincide
#   campo por campo con lo que lee extract_reading() acá abajo
#   (codigo_maquinaria, presion) y con test_event.json. Mismo
#   tópico también recibe ACKs/NACKs de configuración de ese aspersor
#   (otra forma de registro, sin presion, a veces sin
#   codigo_maquinaria) — la IoT Rule (SELECT * FROM 'RIO/ASP/DECODED') no
#   filtra por tipo de mensaje, así que esos SÍ llegan a esta Lambda, pero
#   extract_reading() los descarta de forma limpia (ver su docstring) sin
#   tocar DynamoDB ni publicar nada. No hace falta ningún cambio de código
#   para este contrato — ya está alineado 1:1 con la fuente real.
#
#   Esta Lambda:
#     1. Recibe ese registro (vía una IoT Rule, FROM 'RIO/ASP/DECODED').
#     2. Actualiza en DynamoDB la última presión conocida de ESE aspersor.
#     3. Busca qué motor tiene asignado ese aspersor, y qué otros
#        aspersores están asignados al MISMO motor (GSI motorId-index).
#     4. Descarta lecturas más viejas que STALE_READING_MAX_AGE_SECONDS
#        (un aspersor que dejó de reportar no debe seguir pesando para
#        siempre en la agregación).
#     5. Agrega las lecturas restantes (política configurable, ver
#        AGGREGATION_POLICY).
#     6. Aplica debounce contra el último valor PUBLICADO a ese motor
#        (tabla separada RIO-MotorDownlinkState) — solo publica si cambió
#        lo suficiente o pasó suficiente tiempo.
#     7. Si corresponde publicar, manda {"PRESION_REMOTO": valor} a
#        RIO/<motorId>/downlink — mismo tópico/formato que ya entiende
#        RIO-DSL-SendDownlink, sin tocar esa Lambda.
#        ⚠️ CORREGIDO 2026-10-01: antes mandaba {"PRESION": ...}, pero el ID
#        17 se renombró a PRESION_REMOTO en la PARAMETER_TABLE de
#        RIO-DSL-SendDownlink (junto con el firmware) — con el nombre viejo
#        esa Lambda rechazaba cada publicación ("nombre de parametro
#        desconocido") y el motor no recibía la presión remota. Ver
#        PARAMETRO_PRESION_REMOTA.
#
# ============================================================================
# TABLAS DYNAMODB
# ----------------------------------------------------------------------------
# 1) RIO-AspersorMotorMap (sin cambios de esquema respecto a la v1)
#    PK: aspersorId (S)              — ej. "ASP-0001"
#    Atributos: motorId (S), lastPresion (N), lastUpdatedUnix (N)
#    GSI: motorId-index, PK=motorId (S), proyección ALL
#
# 2) RIO-MotorDownlinkState (NUEVA — estado de debounce, un item por motor)
#    PK: motorId (S)                 — ej. "DSL-0001"
#    Atributos: lastPublishedPresion (N), lastPublishedUnix (N)
#
# Ver table_setup.py (mismo directorio) para crear ambas con boto3 — ya no
# hace falta copiar comandos de CLI a mano.
#
# ============================================================================
# IOT RULE (sin cambios respecto a la v1):
#   Nombre sugerido: RIO_Route_Aspersor_To_Motor
#   SQL: SELECT * FROM 'RIO/ASP/DECODED'
#   Acción: Lambda -> RIO-RouteRelaciones
#
# ============================================================================
# IAM — el rol de ejecución ahora necesita permisos sobre AMBAS tablas:
#
#   {
#     "Version": "2012-10-17",
#     "Statement": [
#       {
#         "Effect": "Allow",
#         "Action": ["dynamodb:UpdateItem", "dynamodb:Query"],
#         "Resource": [
#           "arn:aws:dynamodb:us-east-1:<CUENTA>:table/RIO-AspersorMotorMap",
#           "arn:aws:dynamodb:us-east-1:<CUENTA>:table/RIO-AspersorMotorMap/index/motorId-index"
#         ]
#       },
#       {
#         "Effect": "Allow",
#         "Action": ["dynamodb:GetItem", "dynamodb:UpdateItem"],
#         "Resource": "arn:aws:dynamodb:us-east-1:<CUENTA>:table/RIO-MotorDownlinkState"
#       },
#       {
#         "Effect": "Allow",
#         "Action": "iot:Publish",
#         "Resource": "arn:aws:iot:us-east-1:<CUENTA>:topic/RIO/*/downlink"
#       }
#     ]
#   }
#
# ============================================================================
# DEPENDENCIAS DEL FIRMWARE DEL ASPERSOR (RIO-ASP, proyecto
# "Sensor de Presion" / RAK3372_PCM300_LoRaWAN) — verificadas 2026-09-09
# contra el código fuente real (no asumidas). Nada de esto se resuelve
# desde esta Lambda; se documentan acá porque son supuestos de los que
# depende su comportamiento correcto:
#
#   1. ✅ RESUELTO 2026-09-09: el firmware del ASP ahora expone la falla
#      eléctrica del sensor en el registro plano — RIO-ASP-DecodeUplink
#      agrega dos campos nuevos a la salida en RIO/ASP/DECODED:
#      `falla_sensor` (bool) y `estado_electrico` (string, ej.
#      "DEBAJO_DE_RANGO", clasificación NAMUR del lazo 4-20mA). Esta
#      Lambda ahora descarta la lectura completa (sin tocar DynamoDB) si
#      `falla_sensor` es true — ver extract_reading() y el paso 0 de
#      lambda_handler(). `estado_electrico` es informativo (aparece en
#      el resultado cuando se descarta por esto), no se usa para decidir
#      nada por sí solo: la Lambda confía en el booleano `falla_sensor`
#      que ya calculó el firmware/decoder, no reinterpreta el string.
#   2. `sensorIdNumeric` (-> codigo_maquinaria, ej. "ASP-0001") tiene
#      valor de fábrica 1 en TODO nodo sin configurar o que perdió su
#      flash, sin validación de unicidad en el firmware — es un riesgo
#      100% operativo (aprovisionamiento), no algo que esta Lambda pueda
#      detectar (recibiría lecturas de dos aspersores físicos distintos
#      mezcladas bajo el mismo "ASP-0001").
#   3. standbyTxIntervalMs/activeTxIntervalMs (cadencia de reporte del
#      aspersor) son reconfigurables por downlink, POR NODO, en
#      cualquier momento, sin que esta Lambda se entere. Default de
#      fábrica confirmado: standby=1800000ms (30 min exactos),
#      active=90000ms (1:30 min) — runtime_config.cpp del firmware ASP.
#      Si alguna vez se sube el intervalo standby de un nodo por encima
#      de STALE_READING_MAX_AGE_SECONDS (abajo), esta Lambda empezaría a
#      descartar sistemáticamente sus lecturas como "viejas". Cualquier
#      cambio de esos parámetros en campo debe coordinarse con este
#      valor.
#   4. Bug de infraestructura conocido en el proyecto ASP (documentado
#      ahí, no confirmado si sigue vigente): dos IoT Rules suscritas al
#      mismo tópico RIO/ASP (una con typo en el nombre) pueden causar
#      que cada uplink llegue DUPLICADO a RIO/ASP/DECODED (mismo
#      contenido, mismo instante). El debounce de esta Lambda ya lo
#      absorbe sin cambios: el duplicado llega con delta=0 y
#      elapsed<DEBOUNCE_MIN_INTERVAL_SECONDS contra el valor recién
#      publicado por el primero, así que should_publish() lo descarta
#      solo — no requiere ningún ajuste acá, queda documentado para que
#      un duplicado observado en logs no se investigue como anomalía.
#
# ============================================================================
# DECISIONES ABIERTAS / TODO QUE QUEDAN (el resto ya se cerró en esta v2):
#
#   - AGGREGATION_POLICY sigue en "MIN" por defecto — confirmar con
#     operaciones si es la política correcta a largo plazo.
#   - STALE_READING_MAX_AGE_SECONDS: subido a 2700s (45 min) el
#     2026-09-09 — CORREGIDO, el valor anterior (1800s) estaba basado en
#     un dato equivocado (el comentario del firmware DEL MOTOR decía
#     "15-20 min", pero el default real de reporte standby del ASPERSOR,
#     confirmado en su propio firmware, es 1800s/30min EXACTOS — margen
#     cero contra el propio umbral). 2700s deja ~900s (15 min) de margen
#     sobre ese default de fábrica, suficiente para absorber jitter real
#     (backoff de join hasta 120s/intento, ventana GNSS ~20s, reintentos
#     de envío ~5s). Si el default de fábrica del ASP cambia, revisar
#     este valor junto con la dependencia 3 de arriba.
#   - DEBOUNCE_DELTA_PSI / DEBOUNCE_MIN_INTERVAL_SECONDS son valores de
#     arranque razonables (2.0 psi / 60s), no medidos contra tráfico real
#     todavía — revisar una vez que haya lecturas reales de campo.
# ============================================================================

import json
import os
import time

AGGREGATION_POLICY = "MIN"  # "MIN" | "AVG" | "LAST"

MAP_TABLE_NAME = os.environ.get("MAP_TABLE_NAME", "RIO-AspersorMotorMap")
MOTOR_INDEX_NAME = os.environ.get("MOTOR_INDEX_NAME", "motorId-index")
MOTOR_STATE_TABLE_NAME = os.environ.get("MOTOR_STATE_TABLE_NAME", "RIO-MotorDownlinkState")

STALE_READING_MAX_AGE_SECONDS = int(os.environ.get("STALE_READING_MAX_AGE_SECONDS", "2700"))
DEBOUNCE_DELTA_PSI = float(os.environ.get("DEBOUNCE_DELTA_PSI", "2.0"))
DEBOUNCE_MIN_INTERVAL_SECONDS = int(os.environ.get("DEBOUNCE_MIN_INTERVAL_SECONDS", "60"))

DOWNLINK_TOPIC_TEMPLATE = "RIO/{motor_id}/downlink"
# Nombre del parametro tal como lo espera la PARAMETER_TABLE de
# RIO-DSL-SendDownlink (ID 17, CALIB_ID_PRESION_REMOTO en el firmware DSL).
PARAMETRO_PRESION_REMOTA = "PRESION_REMOTO"


def extract_reading(event):
    """`event` es el registro plano que ya publica RIO-ASP-DecodeUplink en
    RIO/ASP/DECODED. Solo interesan los frames de tracker (traen
    codigo_maquinaria + presion) — los ACK/NACK de configuración
    (mismo tópico, misma Rule, sin filtro por tipo de mensaje) no traen
    presion y se ignoran acá sin que sea un error ni tocar
    DynamoDB/IoT. Verificado contra build_gio_software_record() en
    RIO-ASP-DecodeUplink/decoder.py (2026-09-09) — ver nota en el header
    del archivo. No filtra por `falla_sensor` acá — eso lo hace
    lambda_handler() con un motivo de descarte propio, ver
    is_sensor_fault()."""
    aspersor_id = event.get("codigo_maquinaria")
    presion = event.get("presion")
    if aspersor_id is None or presion is None:
        return None
    return aspersor_id, presion


def is_sensor_fault(event):
    """`falla_sensor` (bool) es un campo nuevo (2026-09-09) que agrega
    RIO-ASP-DecodeUplink cuando el lazo 4-20mA del sensor está en falla
    eléctrica (NAMUR FAULT_LOW/FAULT_HIGH — cable suelto o corto). En
    ese caso `presion` sigue viniendo (el firmware la calcula
    igual con la fórmula lineal normal), pero es un valor sin sentido:
    puede leerse como 0 psi (indistinguible de aspersor apagado) o como
    una presión alta plausible (falso positivo de sobrepresión). No
    confiar en el string `estado_electrico` acá — es solo informativo
    para el resultado, la decisión es 100% el booleano que ya calculó el
    firmware/decoder."""
    return event.get("falla_sensor") is True


def aggregate_pressure(readings, presion_recibida):
    """readings: lista de lastPresion (float) de aspersores del mismo motor
    que pasaron el filtro de frescura. Si queda vacía (todas viejas, o
    carrera rara justo después del primer update_item), se usa solo la
    lectura actual en vez de fallar."""
    if not readings:
        readings = [presion_recibida]

    if AGGREGATION_POLICY == "MIN":
        return min(readings)
    if AGGREGATION_POLICY == "AVG":
        return sum(readings) / len(readings)
    if AGGREGATION_POLICY == "LAST":
        return presion_recibida
    raise ValueError(f"AGGREGATION_POLICY desconocida: {AGGREGATION_POLICY!r}")


def should_publish(motor_state_item, valor_final, now):
    """Decide si vale la pena mandar un downlink nuevo, comparando contra
    el último valor REALMENTE publicado a ese motor (no contra la última
    lectura de un aspersor individual). Sin estado previo (motor nunca
    recibió un downlink de esta Lambda) siempre se publica."""
    if motor_state_item is None:
        return True, "sin_publicacion_previa"

    last_value = motor_state_item.get("lastPublishedPresion")
    last_unix = motor_state_item.get("lastPublishedUnix", 0)

    if last_value is None:
        return True, "sin_valor_previo"

    delta = abs(float(valor_final) - float(last_value))
    if delta >= DEBOUNCE_DELTA_PSI:
        return True, f"cambio_supera_umbral ({delta:.2f} psi >= {DEBOUNCE_DELTA_PSI} psi)"

    elapsed = now - int(last_unix)
    if elapsed >= DEBOUNCE_MIN_INTERVAL_SECONDS:
        return True, f"intervalo_minimo_cumplido ({elapsed}s >= {DEBOUNCE_MIN_INTERVAL_SECONDS}s)"

    return False, f"debounced (delta={delta:.2f}psi, elapsed={elapsed}s)"


def lambda_handler(event, context):
    # import local: boto3 solo existe en el entorno real de Lambda, mismo
    # criterio que el resto del repo.
    import boto3

    if isinstance(event, dict) and isinstance(event.get("Records"), list):
        from ruteo_relaciones import rutear_registro_stream
        ddb = boto3.client("dynamodb")
        iot = boto3.client("iot-data")
        resultados = []
        for registro in event["Records"]:
            resultado = rutear_registro_stream(registro, ddb, iot)
            print(f"[ruteo] {json.dumps(resultado, default=str)}")
            resultados.append(resultado)
        return {"registros": len(resultados), "resultados": resultados}

    return manejar_lectura_rule(event, boto3)


def manejar_lectura_rule(event, boto3):
    """Camino anterior: registro plano de la IoT Rule sobre RIO/ASP/DECODED."""
    reading = extract_reading(event)
    if reading is None:
        return {
            "skipped": True,
            "reason": "evento sin codigo_maquinaria/presion (probablemente un ACK de config, no un frame de tracker)",
        }
    aspersor_id, presion = reading

    # 0. Descartar ANTES de tocar DynamoDB si el sensor reportó falla
    #    eléctrica — presion no es confiable en ese caso (ver
    #    is_sensor_fault()). No se actualiza lastPresion de este
    #    aspersor: se trata igual que si no hubiera reportado nada este
    #    ciclo, para no corromper la agregación con un valor sin sentido.
    if is_sensor_fault(event):
        return {
            "skipped": True,
            "aspersorId": aspersor_id,
            "reason": (
                f"descartado: falla_sensor=true "
                f"(estado_electrico={event.get('estado_electrico')!r}) — "
                "presion no es confiable, no se actualiza DynamoDB"
            ),
        }

    now = int(time.time())

    dynamodb = boto3.resource("dynamodb")
    map_table = dynamodb.Table(MAP_TABLE_NAME)
    state_table = dynamodb.Table(MOTOR_STATE_TABLE_NAME)

    # 1. Actualizar la última lectura conocida de ESTE aspersor (upsert).
    updated = map_table.update_item(
        Key={"aspersorId": aspersor_id},
        UpdateExpression="SET lastPresion = :p, lastUpdatedUnix = :t",
        ExpressionAttributeValues={":p": presion, ":t": now},
        ReturnValues="ALL_NEW",
    )["Attributes"]

    motor_id = updated.get("motorId")
    if motor_id is None:
        return {
            "skipped": True,
            "reason": f"aspersor {aspersor_id} sin motorId asignado en {MAP_TABLE_NAME}",
        }

    # 2. Traer las últimas lecturas de todos los aspersores de ese motor.
    query_result = map_table.query(
        IndexName=MOTOR_INDEX_NAME,
        KeyConditionExpression="motorId = :m",
        ExpressionAttributeValues={":m": motor_id},
    )
    all_items = query_result.get("Items", [])

    # 3. Descartar lecturas viejas antes de agregar.
    fresh_readings = [
        item["lastPresion"] for item in all_items
        if item.get("lastPresion") is not None
        and (now - int(item.get("lastUpdatedUnix", 0))) <= STALE_READING_MAX_AGE_SECONDS
    ]
    stale_count = len(all_items) - len(fresh_readings)

    # 4. Agregar.
    valor_final = aggregate_pressure(fresh_readings, presion)

    # 5. Debounce contra el último valor publicado a ESTE motor.
    motor_state = state_table.get_item(Key={"motorId": motor_id}).get("Item")
    publish, publish_reason = should_publish(motor_state, valor_final, now)

    result = {
        "aspersorId": aspersor_id,
        "motorId": motor_id,
        "presionRecibida": presion,
        "aspersoresConsiderados": len(fresh_readings),
        "aspersoresDescartadosPorViejos": stale_count,
        "politicaAgregacion": AGGREGATION_POLICY,
        "valorAgregado": valor_final,
        "publicado": publish,
        "razonDebounce": publish_reason,
    }

    if not publish:
        return result

    # 6. Publicar el downlink dirigido — mismo tópico/formato que ya
    #    entiende RIO-DSL-SendDownlink.
    iot_data = boto3.client("iot-data")
    topic = DOWNLINK_TOPIC_TEMPLATE.format(motor_id=motor_id)
    payload = json.dumps({PARAMETRO_PRESION_REMOTA: round(float(valor_final), 1)})
    iot_data.publish(topic=topic, qos=1, payload=payload)

    # 7. Guardar el estado de debounce para la próxima vez.
    state_table.update_item(
        Key={"motorId": motor_id},
        UpdateExpression="SET lastPublishedPresion = :p, lastPublishedUnix = :t",
        ExpressionAttributeValues={":p": valor_final, ":t": now},
    )

    result["topicoPublicado"] = topic
    return result