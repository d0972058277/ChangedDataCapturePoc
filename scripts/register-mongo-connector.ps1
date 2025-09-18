param(
    [string]$ConfigPath = (Join-Path $PSScriptRoot '..\debezium\mongodb-source.json'),
    [string]$ConnectUrl = 'http://localhost:38083'
)

Write-Host "Registering MongoDB source connector using $ConfigPath" -ForegroundColor Cyan

Invoke-WebRequest -Uri "$ConnectUrl/connectors" -Method Post -ContentType 'application/json' -InFile $ConfigPath |
    Select-Object -ExpandProperty Content
