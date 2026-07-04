Get-ChildItem -Path "d:\workspace\workspace_tech_yushen1202\tech-mq-rocketmq" -Recurse -Filter "*.java" | ForEach-Object {
    $content = Get-Content $_.FullName -Raw
    if ($content.StartsWith("`r`n") -or $content.StartsWith("`n") -or $content.StartsWith("`r")) {
        $content = $content.TrimStart("`r`n", "`n", "`r")
        [System.IO.File]::WriteAllText($_.FullName, $content, [System.Text.Encoding]::UTF8)
        Write-Host "Fixed: $($_.FullName)"
    }
}
Write-Host "Done"