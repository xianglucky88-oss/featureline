SHELL := /bin/sh
PY ?= python3
BASE ?= http://127.0.0.1:9500
REGISTRY ?= data/registry
BINDING ?= data/binding.json
# 纯 Go 项目，关闭 cgo 以获得无外部工具链依赖的静态构建。
export CGO_ENABLED = 0

.PHONY: help
help:
	@echo "make build    离线构建（事件 + 绑定固定 + 版本化快照）"
	@echo "make serve    起 Go 服务（按 binding.json 固定定义）"
	@echo "make parity   训练/服务一致性对比"
	@echo "make verify   五项一致性断言"
	@echo "make demo     固定演示序列（含注册表治理演练）"
	@echo "make test     Go + Python 单测"
	@echo "make backfill 演示 30 天回填成本"

.PHONY: build
build:
	$(PY) -m featureline build --registry $(REGISTRY) --binding $(BINDING)

.PHONY: serve
serve:
	go run ./cmd/serve -addr 127.0.0.1:9500 \
		-snapshot data/redis_snapshot.json -registry $(REGISTRY) -binding $(BINDING)

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
	go test ./... -count=1
	$(PY) -m pytest -q

.PHONY: backfill
backfill:
	$(PY) -m featureline backfill --days 30
