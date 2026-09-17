"""pytest 公共配置: 项目根入 sys.path,使 tests 可导入 scripts/ 等顶层模块。"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
