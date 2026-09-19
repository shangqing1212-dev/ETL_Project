"""数仓构建编排(M4): ODS → DWD/DWS/ADS 幂等构建;编排核心在 build.py,CLI 见 scripts/build_dw.py。"""

from etl_sdk.dw.build import BuildResult, BuildSummary, load_shop_registry, run_build

__all__ = ["BuildResult", "BuildSummary", "load_shop_registry", "run_build"]
