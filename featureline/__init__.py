"""featureline —— 特征平台。

离线：Polars 按天算特征写 Parquet
线上：Go 服务从 Redis 读特征
训练：读 Parquet

当前状态：离线与线上特征有约 6% 的样本不一致（tools/parity_check 的结论），
线上 AUC 因此掉了 8 个点。

核心问题：两边各自实现了一遍窗口逻辑，且窗口语义不同。
"""

__version__ = "0.8.2"
