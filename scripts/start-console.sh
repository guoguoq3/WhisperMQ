#!/usr/bin/env bash
# 构建并启动 WhisperMQ Web 控制台（内嵌 Broker + 页面）
# 用法：./scripts/start-console.sh
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

echo "构建 WhisperMQ-console ..."
mvn -o -q -DskipTests package -pl WhisperMQ-console -am

echo "控制台启动中：http://127.0.0.1:8080"
echo "内嵌 Broker 端口：9999；按 Ctrl+C 退出"
exec java -jar WhisperMQ-console/target/WhisperMQ-console-1.0-SNAPSHOT.jar
