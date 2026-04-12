# ══════════════════════════════════════════════
# 标普500监控系统 v10.1 — 配置文件
# ══════════════════════════════════════════════

FRED_API_KEY = "2c00933e61200b66630dfdf8341b6c3a"

EMAIL_SENDER   = "zhanghangrui1984@gmail.com"
EMAIL_RECEIVER = "zhanghangrui1984@gmail.com"
EMAIL_PASSWORD = "qwwhzkweasrvthnm"

# 当前持仓状态（空仓填""，持仓填情景名如"情景1A"）
CURRENT_POSITION = ""
ENTRY_DATE       = ""
ENTRY_SP         = 0      # 入场时标普500点位

# SC4系列入场后30日免疫状态（系统自动维护，不需手动修改）
SC4_IMMUNE_UNTIL = ""     # 格式："2026-04-12"，空字符串表示无免疫

# 空仓期TLT持仓状态（系统自动维护）
TLT_HOLDING      = False

LOG_DIR  = "D:\\sp500_monitor\\logs"
DATA_DIR = "D:\\sp500_monitor\\data"
