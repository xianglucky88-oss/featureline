"""featureline —— 特征平台。

离线：Polars 按注册表绑定的定义算特征，写 Parquet 与带清单的版本化快照
线上：Go 服务按同一份绑定清单（Binding）读版本化键
训练：读 Parquet，并固定到绑定的已发布定义（FQVN + 指纹）

定义治理由 internal/registry 提供：草稿、发布、弃用、回退、并行版本，
不兼容变化只升 major 且不自动激活，旧绑定永远可解析、可服务。
"""

__version__ = "1.0.0"
