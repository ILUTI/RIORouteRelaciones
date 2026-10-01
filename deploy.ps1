Set-Location $PSScriptRoot
# Lambda RIO-RouteRelaciones (antes RIO-ASP-ROUTE-MT; runtime Python, Handler: lambda_function.lambda_handler).
Compress-Archive -Path ./lambda_function.py -DestinationPath ./package.zip -Force
Write-Output "package.zip generado en $PSScriptRoot\package.zip"
Write-Output "Subilo manualmente en la consola de AWS Lambda (RIO-RouteRelaciones)."
