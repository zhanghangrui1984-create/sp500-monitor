"""Dependency-free failure notification, also usable if installation or regression checks fail."""
import json, os, smtplib
from pathlib import Path
from email.message import EmailMessage

def main():
    receipt=Path('reports/delivery_receipt.json')
    if receipt.exists() and json.loads(receipt.read_text(encoding='utf8')).get('smtp_accepted'):
        print('An abnormal-state report was already delivered.');return
    sender=os.environ['EMAIL_ADDRESS'];receiver=os.environ.get('EMAIL_RECEIVER') or sender
    msg=EmailMessage();msg['From']=sender;msg['To']=receiver;msg['Subject']='【标普500 v17·系统异常】每日任务未完成'
    url='https://github.com/'+os.environ.get('GITHUB_REPOSITORY','zhanghangrui1984-create/sp500-monitor')+'/actions/runs/'+os.environ.get('GITHUB_RUN_ID','')
    msg.set_content('本次每日任务未能完成有效计算或报告发送，请维持原状态并检查运行记录。\n'+url)
    with smtplib.SMTP_SSL('smtp.gmail.com',465,timeout=40) as s:
        s.login(sender,os.environ['EMAIL_PASSWORD']);rejected=s.send_message(msg)
    if rejected:raise RuntimeError('Failure notice rejected')
    print('SMTP accepted pipeline failure notice.')
if __name__=='__main__':main()
