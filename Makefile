SHELL := /bin/sh
PY ?= python3
BASE ?= http://127.0.0.1:9500

.PHONY: help
help:
	@echo "make build    离线构建（事件 + 特征 + 快照）"
	@echo "make serve    起 Go 服务"
	@echo "make serve-refresh  以新快照为准重启服务"
	@echo "make parity   训练/服务一致性对比"
	@echo "make verify   四项一致性断言"
	@echo "make demo     固定演示序列"
	@echo "make test     Go + Python 单测"
	@echo "make backfill 演示 30 天回填成本"

.PHONY: build
build:
	$(PY) -m featureline build

.PHONY: serve
serve:
	go run ./cmd/serve -addr 127.0.0.1:9500 -store-dir data/store -snapshot data/redis_snapshot.json

# 离线重新产出快照后，用它让服务端以新快照为准（同 key 覆盖，其余保留）。
.PHONY: serve-refresh
serve-refresh:
	go run ./cmd/serve -addr 127.0.0.1:9500 -store-dir data/store \
		-snapshot data/redis_snapshot.json -snapshot-replace

.PHONY: parity
parity:
	$(PY) -m featureline parity --base $(BASE)

.PHONY: verify
verify:
	$(PY) scripts/verify_features.py --base $(BASE)

.PHONY: demo
demo:
	sh scripts/demo.sh

.PHONY: test
test:
	go test ./... -count=1 -race
	$(PY) -m pytest -q

.PHONY: backfill
backfill:
	$(PY) -m featureline backfill --days 30
