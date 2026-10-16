"""
本地从云端 data 分支拉取最新数据
═══════════════════════════════════════════════════════════════════════════

用法:
    python sync_from_cloud.py
    python sync_from_cloud.py --backup   # 先备份本地数据再覆盖

效果:
    把 GitHub data 分支里的 data/ 和 logs/ 目录拉到本地,覆盖现有的
    (前提:本地已有 git 配置,且对仓库有访问权限)

适用场景:
    - 想看云端的最新数据库
    - 本地数据库被弄坏了,想重置
    - 第一次从其他电脑拉取
"""

import os
import sys
import shutil
import subprocess
from datetime import datetime
from pathlib import Path


def run(cmd, check=True, silent=False):
    """运行 shell 命令,返回输出"""
    if not silent:
        print(f"  $ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        if not silent:
            print(f"  [错误] {result.stderr}")
        return None
    return result.stdout.strip()


def is_git_repo():
    """确认当前目录是 git 仓库"""
    return run("git rev-parse --is-inside-work-tree 2>nul || git rev-parse --is-inside-work-tree 2>/dev/null",
               check=False, silent=True) == "true"


def backup_local():
    """备份本地 data/ 和 logs/ 到 backup_<时间戳>/"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"backup_{ts}"
    os.makedirs(backup_dir, exist_ok=True)
    n_data = n_logs = 0
    if os.path.exists("data"):
        shutil.copytree("data", os.path.join(backup_dir, "data"), dirs_exist_ok=True)
        n_data = len(list(Path(f"{backup_dir}/data").glob("*")))
    if os.path.exists("logs"):
        shutil.copytree("logs", os.path.join(backup_dir, "logs"), dirs_exist_ok=True)
        n_logs = len(list(Path(f"{backup_dir}/logs").glob("*")))
    print(f"  ✅ 已备份到 {backup_dir}/ (data: {n_data} 文件, logs: {n_logs} 文件)")


def main():
    print("=" * 60)
    print("从云端 data 分支同步最新数据到本地")
    print("=" * 60)

    if not is_git_repo():
        print("\n❌ 当前目录不是 git 仓库,请在仓库根目录运行")
        sys.exit(1)

    # 是否备份本地
    if '--backup' in sys.argv:
        print("\n[1/3] 备份本地数据...")
        backup_local()
    else:
        print("\n[1/3] 跳过备份(如需备份,加 --backup 参数)")

    print("\n[2/3] 从远端拉 data 分支...")
    run("git fetch origin data", check=False)

    # 检查 data 分支是否存在
    branches = run("git branch -r", silent=True) or ""
    if 'origin/data' not in branches:
        print("\n❌ 远端没有 data 分支,可能云端还没跑过")
        print("   先在 GitHub Actions 手动触发一次,等它跑完再来同步")
        sys.exit(1)

    print("\n[3/3] 把云端 data/ 和 logs/ 检出到本地(只覆盖这两个目录)...")
    # 用 git checkout 从远端分支取出指定路径
    # 这种方式不会切换分支,只把指定目录的文件覆盖到当前工作树
    result_data = run("git checkout origin/data -- data", check=False)
    result_logs = run("git checkout origin/data -- logs", check=False)

    # 统计文件数
    n_data = len(list(Path("data").glob("*"))) if os.path.exists("data") else 0
    n_logs = len(list(Path("logs").glob("*"))) if os.path.exists("logs") else 0

    print()
    print("=" * 60)
    print(f"✅ 同步完成")
    print(f"   data/ : {n_data} 个文件")
    print(f"   logs/ : {n_logs} 个文件")
    print()

    # 显示关键文件信息
    if os.path.exists("data/sp500_realtime_db.csv"):
        import pandas as pd
        try:
            df = pd.read_csv("data/sp500_realtime_db.csv", index_col='date', parse_dates=True)
            print(f"   sp500_realtime_db.csv: {len(df)} 行 ({df.index[0].date()} ~ {df.index[-1].date()})")
        except Exception as e:
            print(f"   [警告] 读 sp500_realtime_db.csv 失败: {e}")
    if os.path.exists("data/sp500_factors_history.csv"):
        try:
            import pandas as pd
            df = pd.read_csv("data/sp500_factors_history.csv")
            print(f"   sp500_factors_history.csv: {len(df)} 行")
        except Exception as e:
            print(f"   [警告] 读 sp500_factors_history.csv 失败: {e}")
    print("=" * 60)
    print()
    print("注意:这些文件**不会**被提交到 main 分支(.gitignore 已排除)")
    print("如需将本地修改推到云端,请用 GitHub Actions 重新触发运行")


if __name__ == '__main__':
    main()
