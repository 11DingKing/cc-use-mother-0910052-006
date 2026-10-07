"""pytest 全局配置。

在任何 app 模块导入前关闭策略后台推进器，时间线推进由测试显式调用，
避免后台线程造成竞态（尤其是异常恢复与定时切换的确定性测试）。
"""

import os

os.environ.setdefault("STRATEGY_TICK_SECONDS", "0")
