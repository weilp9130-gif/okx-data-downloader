"""测试公共路径常量

各 tests/ 子目录统一从本模块导入 FIXTURES 等路径，
避免子目录再移动时 __file__ 推导失效。
"""

from pathlib import Path

TESTS_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = TESTS_ROOT.parent
FIXTURES = TESTS_ROOT / "fixtures"
