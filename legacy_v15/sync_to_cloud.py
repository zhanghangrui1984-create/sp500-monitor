"""
把本地 data/ 和 logs/ 一次性推到云端 data 分支
═══════════════════════════════════════════════════════════════════════════

用途:
    首次部署时,把本地已有的完整数据库推到云端,这样云端
    就不用从零开始累积。**只需要跑一次**。

用法:
    python sync_to_cloud.py

效果:
    在 GitHub 上创建/覆盖 data 分支,内容是本地的 data/ 和 logs/。
    此后云端每天自动跑都会基于这份数据继续累积。
"""

import os
import sys
import shutil
import subprocess
import tempfile
from pathlib import Path


def run(cmd, check=True, silent=False, cwd=None):
    if not silent:
        print(f"  $ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    if check and result.returncode != 0:
        if not silent:
            print(f"  [错误] {result.stderr}")
        return None
    return result.stdout.strip()


def get_remote_url():
    return run("git config --get remote.origin.url", silent=True)


def main():
    print("=" * 60)
    print("把本地数据一次性推到云端 data 分支(首次部署用)")
    print("=" * 60)

    if not os.path.exists("data") or not os.listdir("data"):
        print("\n❌ 本地 data/ 目录不存在或为空,没数据可推")
        sys.exit(1)

    remote = get_remote_url()
    if not remote:
        print("\n❌ 当前目录不是 git 仓库")
        sys.exit(1)
    print(f"\n远端仓库:{remote}")

    # 确认
    print("\n⚠️ 这个操作会:")
    print("   1. 覆盖云端 GitHub 上的 data 分支(如果存在)")
    print("   2. 把本地 data/ 和 logs/ 完整传上去")
    print("   3. main 分支(代码)不会受影响")
    ans = input("\n确认继续吗?(y/N): ").strip().lower()
    if ans not in ('y', 'yes'):
        print("已取消")
        sys.exit(0)

    # 用临时目录做一个干净的 orphan 分支推送
    print("\n[1/4] 建临时工作目录...")
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"  临时目录: {tmpdir}")

        print("\n[2/4] 复制 data/ 和 logs/ 到临时目录...")
        shutil.copytree("data", os.path.join(tmpdir, "data"))
        n_data = len(list(Path("data").glob("*")))
        print(f"  data/: {n_data} 个文件")
        if os.path.exists("logs"):
            shutil.copytree("logs", os.path.join(tmpdir, "logs"))
            n_logs = len(list(Path("logs").glob("*")))
            print(f"  logs/: {n_logs} 个文件")

        print("\n[3/4] 在临时目录初始化 git 并连到远端...")
        run("git init", cwd=tmpdir)
        run("git checkout -b data", cwd=tmpdir)
        run(f'git remote add origin {remote}', cwd=tmpdir)
        run('git config user.email "local-sync@local"', cwd=tmpdir)
        run('git config user.name "Local Sync"', cwd=tmpdir)

        print("\n[4/4] commit + force push 到 data 分支...")
        run("git add .", cwd=tmpdir)
        run('git commit -m "初始化 data 分支:从本地推送" -q', cwd=tmpdir)
        result = run("git push origin data --force", cwd=tmpdir, check=False)
        if result is None:
            print("\n❌ 推送失败,可能原因:")
            print("   - 没有仓库 push 权限")
            print("   - 网络问题")
            print("   - 需要先 git push 用浏览器认证")
            sys.exit(1)

    print()
    print("=" * 60)
    print("✅ 推送完成")
    print("=" * 60)
    print()
    print("现在打开 GitHub → 切到 data 分支,应该能看到 data/ 和 logs/ 目录")
    print()
    print("以后云端 Action 跑会基于这份数据继续累积。")
    print("本地任何修改不会自动同步到云端 — 真要再同步,再跑一次本脚本。")


if __name__ == '__main__':
    main()
