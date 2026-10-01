Set-Location $PSScriptRoot
# Lambda RIO-RouteRelaciones (antes RIO-ASP-ROUTE-MT; runtime Python, Handler: lambda_function.lambda_handler).
# Sin cache de Python ni zip anterior (el zip lleva solo los .py listados)
Get-ChildItem -Recurse -Directory -Filter __pycache__ | Remove-Item -Recurse -Force
Remove-Item ./package.zip -Force -ErrorAction SilentlyContinue
Compress-Archive -Path ./lambda_function.py,./ruteo_relaciones.py -DestinationPath ./package.zip -Force
Write-Output "package.zip generado en $PSScriptRoot\package.zip"
Write-Output "Subilo manualmente en la consola de AWS Lambda (RIO-RouteRelaciones)."
