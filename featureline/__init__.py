"""featureline —— 特征平台。

离线：Polars 按天算特征写 Parquet
线上：Go 服务从 Redis 读特征
训练：读 Parquet

离线回填见 featureline/backfill.py：时间点正确（[t-W, t)，t=标签时点）、
增量预聚合 tiles、迟到事件只重建受影响分区、按标签日分区输出且版本可追溯。
"""

__version__ = "0.8.2"
