# Preuves demandées dans le livrable : arborescence MinIO + offsets Kafka.
# Usage :  .\scripts\proofs.ps1
$mc = Join-Path $PSScriptRoot "mc.exe"
if (-not (Test-Path $mc)) {
  Invoke-WebRequest https://dl.min.io/client/mc/release/windows-amd64/mc.exe -OutFile $mc
}
& $mc alias set local http://localhost:9000 minioadmin minioadmin | Out-Null
Write-Host "`n===== mc ls --recursive local/lake/ =====" -ForegroundColor Cyan
& $mc ls --recursive local/lake/

Write-Host "`n===== kafka-consumer-groups --list =====" -ForegroundColor Cyan
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --list
Write-Host "`n===== kafka-consumer-groups --describe (groupe pedali-demo) =====" -ForegroundColor Cyan
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group pedali-demo
Write-Host "`n===== fin de log par partition (kafka-get-offsets) =====" -ForegroundColor Cyan
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic trips --time -1
