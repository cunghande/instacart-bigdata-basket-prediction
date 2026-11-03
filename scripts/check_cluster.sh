#!/usr/bin/env bash
# Read-only checks to run on Master before submitting the pipeline.
set -u
echo '=== Host ==='
hostname
free -h
df -h .
echo '=== DNS / hosts ==='
getent hosts master slave01 slave02 || true
echo '=== Tools ==='
for tool in java python3 hdfs yarn spark-submit; do
  if command -v "$tool" >/dev/null; then
    echo "$tool: $(command -v "$tool")"
  else
    echo "$tool: MISSING"
  fi
done
echo '=== HDFS ==='
if command -v hdfs >/dev/null; then
  hdfs dfsadmin -report | head -n 35
  hdfs dfs -df -h / || true
fi
echo '=== YARN ==='
if command -v yarn >/dev/null; then
  yarn node -list || true
fi
