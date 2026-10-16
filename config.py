"""Legacy import compatibility. Credentials come only from environment variables."""
import os
FRED_API_KEY=os.environ.get('FRED_API_KEY','')
EMAIL_SENDER=os.environ.get('EMAIL_ADDRESS','')
EMAIL_RECEIVER=os.environ.get('EMAIL_RECEIVER') or EMAIL_SENDER
EMAIL_PASSWORD=os.environ.get('EMAIL_PASSWORD','')
CURRENT_POSITION=''
ENTRY_DATE=''
ENTRY_SP=0
SC4_IMMUNE_UNTIL=''
TLT_HOLDING=False
LOG_DIR='logs'
DATA_DIR='data'
