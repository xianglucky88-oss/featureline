#!/bin/sh
# demo.sh —— 固定演示序列。
#
#   1. 离线 build（注册表播种 + 绑定固定 + 版本化快照）
#   2. 起 Go 服务（启动时解析绑定，失败即拒绝启动）
#   3. 单个实体的返回：每个值都带 FQVN 与绑定指纹
#   4. 五项一致性断言全绿
#   5. 注册表治理演练：兼容发布 1.1、不兼容发布 2.0（不自动激活）、显式切换、回退、弃用
set -eu
cd "$(dirname "$0")/.."

BASE="${BASE:-http://127.0.0.1:9500}"
say() { printf '\n=== %s ===\n' "$1"; }

say "1. 离线构建（固定绑定 + 版本化快照）"
python3 -m featureline build

say "2. 起服务"
mkdir -p bin
# 纯 Go 项目，关闭 cgo 以获得无外部工具链依赖的静态构建。
CGO_ENABLED=0 go build -o bin/featureline ./cmd/serve
./bin/featureline -addr 127.0.0.1:9500 \
  -snapshot data/redis_snapshot.json \
  -registry data/registry -binding data/binding.json &
SRV=$!
trap 'kill $SRV 2>/dev/null || true' EXIT
for i in $(seq 1 25); do
  curl -fsS -o /dev/null "$BASE/healthz" 2>/dev/null && break
  sleep 1
done

say "3. 单个实体的返回（注意每个特征的 FQVN 与绑定指纹）"
curl -fsS "$BASE/v1/features/U0001" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('绑定清单     :', d['manifest_id'], d['binding_fingerprint'][:12])
dbg = next(iter(d['debug'].values()))
print('窗口         :', dbg['window_start'], '->', dbg['window_end'])
for name, fqvn in list(d['versions'].items())[:3]:
    print(f'  {name:26} -> {fqvn} = {d[\"features\"][name]}')
"

say "4. 五项一致性断言"
python3 scripts/verify_features.py --base "$BASE"

say "5. 注册表治理演练"
python3 scripts/registry_demo.py --base "$BASE"

echo
