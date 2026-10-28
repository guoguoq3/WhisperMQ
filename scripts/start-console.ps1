#!/usr/bin/env pwsh
# 构建并启动 WhisperMQ Web 控制台（内嵌 Broker + 页面）
# 用法：./scripts/start-console.ps1

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    Write-Host '构建 WhisperMQ-console ...' -ForegroundColor Cyan
    & mvn -o -q -DskipTests package -pl WhisperMQ-console -am
    if ($LASTEXITCODE -ne 0) {
        throw "Maven 构建失败（exit=$LASTEXITCODE）"
    }
    Write-Host '控制台启动中：http://127.0.0.1:8080' -ForegroundColor Green
    Write-Host '内嵌 Broker 端口：9999；按 Ctrl+C 退出' -ForegroundColor DarkGray
    & java -jar 'WhisperMQ-console/target/WhisperMQ-console-1.0-SNAPSHOT.jar'
} finally {
    Pop-Location
}
