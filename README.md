# featureline

推荐系统的特征平台。离线用 Polars 算特征写 Parquet，线上用 Go 服务从 Redis 读，
训练脚本读 Parquet。

**当前状态：线上 AUC 掉了 8 个点。模型没换、数据没变，问题在特征——
离线算的和线上算的不是一回事。**

## 快速开始

```bash
# 1. 装依赖
pip install -r requirements.txt

# 2. 离线：生成数据 + 算特征 + 导出快照
python -m featureline build

# 3. 线上：起 Go 服务（读快照）
go build -o bin/featureline ./cmd/serve
./bin/featureline -addr :9500 -snapshot data/redis_snapshot.json

# 4. 对比
python -m featureline parity --base http://localhost:9500
python3 scripts/verify_features.py --base http://localhost:9500
make demo    # 固定演示序列
```

不需要 PostgreSQL / 真实 Redis：特征存储是内存实现 + JSON 快照，
起服务只要 2 秒。

## 结构

```
cmd/serve/            Go 服务入口
internal/feature/     特征注册表（Go 与 Python 的唯一契约）
internal/store/       特征存储（内存 + 快照落盘）
internal/online/      线上特征计算（缺陷集中在这里）
featureline/
    offline.py        Polars 离线管线
    parity.py         训练/服务一致性对比
    __main__.py       CLI: build / export / parity / backfill
tests/                Python 单测
scripts/
    verify_features.py  四项一致性断言
    demo.sh             固定演示
data/                 Parquet + 快照（生成物）
```

## 六个特征

| 特征 | 窗口 | 类型 |
|---|---|---|
| `user_7d_order_cnt` | 7 天 | count |
| `user_7d_gmv_cents` | 7 天 | numeric |
| `user_30d_order_cnt` | 30 天 | count |
| `item_7d_ctr` | 7 天 | ratio |
| `item_7d_exposure` | 7 天 | count |
| `user_category_affinity` | 30 天 | categorical |

## 核心问题：四类漂移

`scripts/verify_features.py` 输出四项，当前**全部 FAIL**：

| 断言 | 现状 |
|---|---|
| `V1/snapshot-fresh` | 10/10 实体的窗口内 0 条事件，却返回非零特征值 |
| `V2/window-convention` | 对齐 as_of 后仍不一致（窗口开闭 + 缺失值语义） |
| `V3/missing-semantics` | 缺失特征线上打点 NaN，训练侧看到 0 |
| `V4/time-base` | 服务 as_of 与快照时间相差 **661 小时** |

V4 是最直接的证据：线上服务用"现在"作为窗口右端，但读的是三周前的快照。
窗口是装饰，真正被读的是过期值。

## 窗口语义对照

| | 离线 | 线上 |
|---|---|---|
| 窗口右端 | 数据的 `max(event_time)` | `time.Now()` |
| 区间 | `[T-7d, T]` 左闭右闭 | `[T-7d, T)` 左闭右开 |
| 缺失值 | 填 0 | 填 NaN 后当 0，打点 NaN |
| 时间字段 | `event_time` | 写入时间 |

## 回填成本

```bash
python -m featureline backfill --days 30
```

当前实现逐日全量重算 30 天窗口。生产数据量下实测约 **6 小时**。

## 测试

```bash
python -m pytest -q     # 13 passed, 1 skipped
go test ./...           # online 包全过
```

Go 侧 11 个用例覆盖：缺失特征默认值、NaN 进 debug、NaN 序列化成 null、
窗口用墙钟、WindowFor、存储读取、NaN 清洗、注册表完整性。
Python 侧 14 个覆盖：生成器确定性、事件排序、窗口开闭差异、as_of 控制窗口、
远未来窗口为空、30d ≥ 7d、缺失填 0、CTR 范围、纳秒时间戳解析。
