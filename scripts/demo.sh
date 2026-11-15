#!/bin/sh
# demo.sh —— 90 秒内录完的固定演示。
#
#   1. 离线 build（生成数据 + 特征 + 快照）
#   2. 起 Go 服务
#   3. 看一个实体的返回：窗口是"最近 7 天"，值却是 3 周前的
#   4. 四项断言全红
#   5. 回填成本
set -eu
cd "$(dirname "$0")/.."

BASE="${BASE:-http://127.0.0.1:9500}"
say() { printf '\n=== %s ===\n' "$1"; }

say "1. 离线构建"
python3 -m featureline build

say "2. 起服务"
mkdir -p bin
go build -o bin/featureline ./cmd/serve
# 演示每次从空存储开始，确保导入的是本次 build 生成的快照。
rm -rf data/store
./bin/featureline -addr 127.0.0.1:9500 -store-dir data/store -snapshot data/redis_snapshot.json &
SRV=$!
trap 'kill $SRV 2>/dev/null || true' EXIT
for i in $(seq 1 25); do
  curl -fsS -o /dev/null "$BASE/healthz" 2>/dev/null && break
  sleep 1
done

say "3. 单个实体的返回（注意 as_of 与 updated_at 的差距）"
curl -fsS "$BASE/v1/features/U0001" | python3 -c "
import json,sys
d = json.load(sys.stdin)
print('服务 as_of   :', d['as_of'])
dbg = d['debug']['user_7d_order_cnt']
print('窗口         :', dbg['window_start'], '->', dbg['window_end'])
print('快照写入时间 :', dbg.get('updated_at'))
print('返回的特征   :', d['features'])
"

say "4. 四项一致性断言"
python3 scripts/verify_features.py --base "$BASE" || true

say "5. 回填成本"
python3 -m featureline backfill --days 30

echo
