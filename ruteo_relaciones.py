# ============================================================================
# ruteo_relaciones.py — ruteo por relaciones del espejo de GIOMaps (2026-10-01)
# ----------------------------------------------------------------------------
# Camino nuevo de RIO-RouteRelaciones: disparado por el DynamoDB Stream de la
# tabla de estado en vivo (GIOEstadoVivo), con las relaciones configuradas en
# GIOMaps (maestro de relaciones entre nodos, publicado en el espejo).
#
# Por cada nodo que reporta (item RIO#{WirelessDeviceId} del stream):
#   1. Espejo: WID#{wid}/META -> DevEui; DEV#{DevEui} REL#OUT#{CLAVE}#... ->
#      nodos destino de la relacion (ej. el controlador del motor) con sus
#      Parametros.
#   2. Espejo: DEV#{DevEuiDestino} REL#IN#{CLAVE}#... -> todos los nodos
#      origen de ese destino (ej. todos los aspersores de ese motor).
#   3. Estado en vivo: ultima lectura de cada origen (BatchGetItem).
#   4. Descarta lecturas viejas o con falla de sensor, agrega segun la
#      politica, aplica debounce contra lo ultimo publicado a ese destino y
#      publica {parametro_destino: valor} en RIO/{CodigoDestino}/downlink —
#      mismo topico que ya entiende la SendDownlink del destino.
#
# Parametros de la relacion (ParametrosSchema del tipo de relacion en GIOMaps);
# los que falten toman DEFAULTS:
#   variable_origen    campo del estado del origen (presion)
#   parametro_destino  parametro del downlink al destino (PRESION_REMOTO)
#   politica           MIN | AVG | LAST
#   delta              cambio minimo para publicar antes del intervalo
#   intervalo_min_s    tiempo minimo entre publicaciones sin cambio
#   antiguedad_max_s   lecturas mas viejas no cuentan
#
# Variables de entorno: GIOMAPS_TABLA_MAESTROS (espejo), GIOMAPS_TABLA_ESTADO
# (estado en vivo), RELACION_CLAVE (tipo de relacion, default PRESION_REMOTA),
# MOTOR_STATE_TABLE_NAME (debounce, la misma tabla del camino anterior).
# ============================================================================

import json
import os
import time
from decimal import Decimal

RELACION_CLAVE = os.environ.get("RELACION_CLAVE", "PRESION_REMOTA")
STATE_TABLE = os.environ.get("MOTOR_STATE_TABLE_NAME", "RIO-MotorDownlinkState")
DOWNLINK_TOPIC_TEMPLATE = "RIO/{codigo}/downlink"

DEFAULTS = {
    "variable_origen": "presion",
    "parametro_destino": "PRESION_REMOTO",
    "politica": "MIN",
    "delta": 2.0,
    "intervalo_min_s": 60,
    "antiguedad_max_s": 2700,
}


def _plano(valor):
    if isinstance(valor, Decimal):
        return int(valor) if valor == valor.to_integral_value() else float(valor)
    if isinstance(valor, dict):
        return {k: _plano(v) for k, v in valor.items()}
    if isinstance(valor, list):
        return [_plano(v) for v in valor]
    return valor


def _deserializar(item):
    from boto3.dynamodb.types import TypeDeserializer
    d = TypeDeserializer()
    return _plano({k: d.deserialize(v) for k, v in (item or {}).items()})


def _query_prefijo(ddb, tabla, pk, prefijo):
    items, kwargs = [], {}
    while True:
        res = ddb.query(TableName=tabla, KeyConditionExpression="PK = :pk AND begins_with(SK, :p)",
                        ExpressionAttributeValues={":pk": {"S": pk}, ":p": {"S": prefijo}}, **kwargs)
        items += [_deserializar(i) for i in res.get("Items", [])]
        if "LastEvaluatedKey" not in res:
            return items
        kwargs["ExclusiveStartKey"] = res["LastEvaluatedKey"]


def _estados(ddb, tabla, wids):
    estados = {}
    claves = [{"PK": {"S": f"RIO#{w}"}} for w in wids]
    for i in range(0, len(claves), 100):
        pedido = {tabla: {"Keys": claves[i:i + 100]}}
        while pedido:
            res = ddb.batch_get_item(RequestItems=pedido)
            for item in res.get("Responses", {}).get(tabla, []):
                plano = _deserializar(item)
                estados[plano.get("WirelessDeviceId")] = plano
            pedido = res.get("UnprocessedKeys") or None
    return estados


def _agregar(valores, politica, valor_actual):
    if politica == "AVG":
        return sum(valores) / len(valores)
    if politica == "LAST":
        return valor_actual if valor_actual is not None else valores[-1]
    return min(valores)


def rutear_registro_stream(registro, ddb, iot, ahora=None):
    """Procesa un registro del DynamoDB Stream. Devuelve un dict con el resultado."""
    ahora = ahora or time.time()
    imagen = (registro.get("dynamodb") or {}).get("NewImage")
    if not imagen:
        return {"skipped": True, "reason": "registro del stream sin NewImage"}
    estado = _deserializar(imagen)
    wid = estado.get("WirelessDeviceId")
    if estado.get("falla_sensor") is True:
        return {"skipped": True, "nodo": estado.get("codigo_maquinaria"), "reason": "falla_sensor=true, lectura no confiable"}

    tabla_maestros = os.environ.get("GIOMAPS_TABLA_MAESTROS")
    tabla_estado = os.environ.get("GIOMAPS_TABLA_ESTADO", "GIOEstadoVivo")
    if not tabla_maestros or not wid:
        return {"skipped": True, "reason": "sin GIOMAPS_TABLA_MAESTROS o sin WirelessDeviceId"}

    meta = _deserializar(ddb.get_item(TableName=tabla_maestros,
                                      Key={"PK": {"S": f"WID#{wid}"}, "SK": {"S": "META"}}).get("Item"))
    if not meta.get("DevEui"):
        return {"skipped": True, "wid": wid, "reason": "nodo no registrado en el espejo de GIOMaps"}
    relaciones = _query_prefijo(ddb, tabla_maestros, f"DEV#{meta['DevEui']}", f"REL#OUT#{RELACION_CLAVE}#")
    if not relaciones:
        return {"skipped": True, "nodo": meta.get("Codigo"), "reason": f"sin relaciones {RELACION_CLAVE} salientes"}

    resultados = []
    for rel in relaciones:
        destino = rel.get("Destino") or {}
        params = {**DEFAULTS, **(rel.get("Parametros") or {})}
        origenes = _query_prefijo(ddb, tabla_maestros, f"DEV#{destino.get('DevEui')}", f"REL#IN#{RELACION_CLAVE}#")
        wids = [o["Origen"]["AwsWirelessDeviceId"] for o in origenes if (o.get("Origen") or {}).get("AwsWirelessDeviceId")]
        estados = _estados(ddb, tabla_estado, wids)
        var = params["variable_origen"]
        maximo_ms = float(params["antiguedad_max_s"]) * 1000
        lecturas = [e[var] for e in estados.values()
                    if e.get(var) is not None and e.get("falla_sensor") is not True
                    and ahora * 1000 - (e.get("recibido_epoch") or 0) <= maximo_ms]
        resultado = {"origen": meta.get("Codigo"), "destino": destino.get("Codigo"),
                     "origenesConsiderados": len(lecturas), "origenesRelacionados": len(wids)}
        if not lecturas:
            resultados.append({**resultado, "publicado": False, "razon": "sin lecturas frescas"})
            continue
        valor = round(float(_agregar(lecturas, params["politica"], estado.get(var))), 1)

        # Debounce contra lo ultimo publicado a ese destino (misma tabla que el camino anterior)
        previo = _deserializar(ddb.get_item(TableName=STATE_TABLE,
                                            Key={"motorId": {"S": destino["Codigo"]}}).get("Item"))
        ultimo_valor, ultimo_epoch = previo.get("lastPublishedPresion"), previo.get("lastPublishedUnix", 0)
        if ultimo_valor is not None and abs(valor - float(ultimo_valor)) < float(params["delta"]) \
                and ahora - int(ultimo_epoch) < int(params["intervalo_min_s"]):
            resultados.append({**resultado, "valor": valor, "publicado": False, "razon": "debounce"})
            continue

        topico = DOWNLINK_TOPIC_TEMPLATE.format(codigo=destino["Codigo"])
        iot.publish(topic=topico, qos=1, payload=json.dumps({params["parametro_destino"]: valor}))
        ddb.update_item(TableName=STATE_TABLE, Key={"motorId": {"S": destino["Codigo"]}},
                        UpdateExpression="SET lastPublishedPresion = :p, lastPublishedUnix = :t",
                        ExpressionAttributeValues={":p": {"N": str(valor)}, ":t": {"N": str(int(ahora))}})
        resultados.append({**resultado, "valor": valor, "publicado": True, "topico": topico,
                           "parametro": params["parametro_destino"]})
    return {"nodo": meta.get("Codigo"), "relaciones": resultados}
