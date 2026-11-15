SHELL := /bin/sh
PY ?= python3
BASE ?= http://127.0.0.1:9500

.PHONY: help
help:
	@echo "make build    离线构建（事件 + 特征 + 快照）"
	@echo "make serve    起 Go 服务"
	@echo "make parity   训练/服务一致性对比"
	@echo "make gate     发布前一致性门禁（0 放行 / 1 漂移 / 2 无法结论）"
	@echo "make verify   四项一致性断言"
	@echo "make demo     固定演示序列"
	@echo "make test     Go + Python 单测"
	@echo "make backfill 演示 30 天回填成本"

.PHONY: build
build:
	$(PY) -m featureline build

.PHONY: serve
serve:
	go run ./cmd/serve -addr 127.0.0.1:9500 -snapshot data/redis_snapshot.json

.PHONY: parity
parity:
	$(PY) -m featureline parity --base $(BASE)

.PHONY: gate
gate:
	$(PY) -m featureline gate --base $(BASE)

.PHONY: verify
verify:
	$(PY) scripts/verify_features.py --base $(BASE)

.PHONY: demo
demo:
	sh scripts/demo.sh

.PHONY: test
test:
	go test ./... -count=1
	$(PY) -m pytest -q

.PHONY: backfill
backfill:
	$(PY) -m featureline backfill --days 30
