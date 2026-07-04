$filePath = "d:\workspace\workspace_tech_yushen1202\tech-mq-rocketmq\rocketmq-demo\src\main\java\com\example\rocketmq\controller\MessageController.java"
$bytes = [System.IO.File]::ReadAllBytes($filePath)
if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
    [System.IO.File]::WriteAllBytes($filePath, $bytes[3..($bytes.Length-1)])
    Write-Host "Removed BOM"
} else {
    Write-Host "No BOM found"
}