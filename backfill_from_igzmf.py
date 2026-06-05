#!/usr/bin/env python3
"""
从远程 igzmf 服务器补本地缺失的 tick 和 account_snapshots 数据。

用法:
  python backfill_from_igzmf.py --dry-run           # 仅查看概况
  python backfill_from_igzmf.py --force              # 跳过确认直接执行
  python backfill_from_igzmf.py --force --only MNQ   # 只补指定品种
"""

import argparse
import shlex
import subprocess
import sys
from datetime import datetime, timezone

# ---------- 配置 ----------
LOCAL_DOCKER = "ibkrdata-timescaledb-1"
REMOTE_SSH = "igzmf"
REMOTE_DOCKER = "ibkrdata-timescaledb-1"
DB_USER = "ibkr"
DB_NAME = "ibkrdata"

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ANALYSIS_START = f"{TODAY} 03:00:00+00"
ANALYSIS_END = f"{TODAY} 05:00:00+00"

TICK_SYMBOLS = [
    "10Y", "AUD.USD", "MES", "MNQ", "MYM",
    "N225M", "SPI", "USD.JPY", "ZC",
]


def run_shell(cmd_str, input_data=None, timeout=300):
    try:
        r = subprocess.run(
            cmd_str, input=input_data, capture_output=True,
            text=True, timeout=timeout, shell=True,
        )
        if r.returncode != 0:
            stderr = r.stderr.strip()[:300]
            if stderr:
                print(f"     ⚠️  {stderr}")
            return False, r.stdout.strip()
        return True, r.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, "(timeout)"


def local_sql(sql):
    cmd = f"docker exec {LOCAL_DOCKER} psql -U {DB_USER} -d {DB_NAME} -Atc {shlex.quote(sql)}"
    ok, out = run_shell(cmd)
    return out


def remote_sql(sql):
    remote_cmd = f"docker exec {REMOTE_DOCKER} psql -U {DB_USER} -d {DB_NAME} -Atc {shlex.quote(sql)}"
    cmd = f"ssh {REMOTE_SSH} {shlex.quote(remote_cmd)}"
    ok, out = run_shell(cmd)
    return out


def remote_export_csv(sql):
    """Export data from remote via COPY TO STDOUT WITH CSV, return CSV text."""
    remote_cmd = f"docker exec -i {REMOTE_DOCKER} psql -U {DB_USER} -d {DB_NAME} -c {shlex.quote(sql)}"
    cmd = f"ssh {REMOTE_SSH} {shlex.quote(remote_cmd)}"
    ok, out = run_shell(cmd, timeout=300)
    if not ok:
        return ""
    return out


def local_import_csv(copy_cmd, csv_data):
    """Import CSV via psql stdin. Returns (ok, message)."""
    full_input = f"{copy_cmd}\n{csv_data}"
    cmd = f"docker exec -i {LOCAL_DOCKER} psql -U {DB_USER} -d {DB_NAME}"
    ok, out = run_shell(cmd, input_data=full_input, timeout=300)
    if ok and "COPY" in out:
        parts = out.split()
        count = parts[-1] if parts else "?"
        return True, count
    return False, (out or "error")[:200]


# ---- ticks ----

def backfill_ticks(symbols, dry_run, force):
    print(f"\n{'='*50}")
    print("📊 Ticks 数据补数")
    print(f"{'='*50}\n")

    need = []
    for sym in symbols:
        lc = local_sql(
            f"SELECT count(*) FROM ticks WHERE symbol='{sym}' "
            f"AND time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}'"
        )
        rc = remote_sql(
            f"SELECT count(*) FROM ticks WHERE symbol='{sym}' "
            f"AND time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}'"
        )
        li = int(lc) if lc else 0
        ri = int(rc) if rc else 0
        diff = ri - li
        if diff > 0:
            status = f"⚠️  缺 {diff} 行"
            need.append(sym)
        elif diff == 0 and ri > 0:
            status = "✅ 一致"
        else:
            status = f"📭 本地={li} 远程={ri}"
        print(f"  {sym:<10} 本地={li:<8} 远程={ri:<8} {status}")

    if not need:
        print("\n🎉 Ticks 无需补数\n")
        return

    if dry_run:
        print(f"\n📌 需补 {len(need)} 个: {', '.join(need)}\n")
        return

    if not force:
        ans = input(f"\n⚠️  补 ticks ({len(need)} 个品种)? [y/N] ").strip().lower()
        if ans not in ("y", "yes"):
            print("  跳过 ticks\n"); return

    for sym in need:
        print(f"\n--- {sym} ---")

        export_sql = (
            f"COPY (SELECT time, symbol, bid, ask, last, volume, open, high, low, close "
            f"FROM ticks WHERE symbol='{sym}' "
            f"AND time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}' "
            f"ORDER BY time) TO STDOUT WITH CSV"
        )
        csv_data = remote_export_csv(export_sql)
        if not csv_data.strip():
            print(f"  ⏭️  远程无数据"); continue
        rows = len(csv_data.strip().split("\n"))
        print(f"  📥 远程导出 {rows} 行")

        del_cnt = local_sql(
            f"DELETE FROM ticks WHERE symbol='{sym}' "
            f"AND time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}'"
        )
        if del_cnt and "DELETE" in del_cnt:
            try:
                print(f"  🗑️  删除本地: {int(del_cnt.split()[-1])} 行")
            except (ValueError, IndexError):
                pass

        copy_cmd = (
            "\\COPY ticks (time, symbol, bid, ask, last, volume, open, high, low, close) "
            "FROM STDIN WITH CSV"
        )
        ok, cnt = local_import_csv(copy_cmd, csv_data)
        print(f"  {'✅' if ok else '⚠️'} 导入: {cnt} 行" if ok else f"  ⚠️  导入异常: {cnt}")

        v = local_sql(
            f"SELECT count(*) FROM ticks WHERE symbol='{sym}' "
            f"AND time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}'"
        )
        print(f"  📊 验证: {v} 行")

    # Final verification
    print("\n📊 Ticks 最终验证:")
    all_ok = True
    for sym in symbols:
        lc = local_sql(
            f"SELECT count(*) FROM ticks WHERE symbol='{sym}' "
            f"AND time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}'"
        )
        rc = remote_sql(
            f"SELECT count(*) FROM ticks WHERE symbol='{sym}' "
            f"AND time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}'"
        )
        li = int(lc) if lc else 0
        ri = int(rc) if rc else 0
        ok = li == ri
        if not ok:
            all_ok = False
        print(f"  {'✅' if ok else '⚠️'} {sym:<10} 本地={li:<8} 远程={ri:<8}")
    print(f"  {'🎉 全部一致' if all_ok else '⚠️ 仍有差异'}")


# ---- account_snapshots ----

ACCT_COLUMNS = [
    "time", "account_id", "net_liquidation", "total_cash",
    "available_funds", "excess_liquidity", "init_margin_req",
    "maint_margin_req", "daily_pnl", "unrealized_pnl", "realized_pnl",
]
ACCT_COL_LIST = ", ".join(ACCT_COLUMNS)


def backfill_account_snapshots(dry_run, force):
    print(f"\n{'='*50}")
    print("📊 Account Snapshots 数据补数")
    print(f"{'='*50}\n")

    # Find the actual gap: compare hourly counts
    lc = local_sql(
        f"SELECT count(*) FROM account_snapshots "
        f"WHERE time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}'"
    )
    rc = remote_sql(
        f"SELECT count(*) FROM account_snapshots "
        f"WHERE time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}'"
    )
    li = int(lc) if lc else 0
    ri = int(rc) if rc else 0

    # Broader check: full range since 00:00 UTC today
    lc_full = local_sql(
        f"SELECT count(*) FROM account_snapshots "
        f"WHERE time >= '{TODAY} 00:00:00+00'"
    )
    rc_full = remote_sql(
        f"SELECT count(*) FROM account_snapshots "
        f"WHERE time >= '{TODAY} 00:00:00+00'"
    )
    li_full = int(lc_full) if lc_full else 0
    ri_full = int(rc_full) if rc_full else 0

    print(f"  分析时段 {ANALYSIS_START}~{ANALYSIS_END}:  本地={li:<8} 远程={ri:<8}")
    print(f"  今日 00:00+00 至今:               本地={li_full:<8} 远程={ri_full:<8}")

    if ri <= li:
        print("\n🎉 无需补数\n")
        return

    # Determine actual range — find when local gaps start
    gap_info = remote_sql(
        f"SELECT min(time)::text, max(time)::text, count(*) FROM account_snapshots "
        f"WHERE time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}'"
    )
    print(f"  远程缺口范围: {gap_info}")

    if dry_run:
        diff = ri - li
        print(f"\n📌 需补 {diff} 行 account_snapshots (范围: {ANALYSIS_START}~{ANALYSIS_END})\n")
        return

    if not force:
        ans = input(f"\n⚠️  补 account_snapshots ({(ri-li)} 行)? [y/N] ").strip().lower()
        if ans not in ("y", "yes"):
            print("  跳过 account_snapshots\n"); return

    # Export from remote
    export_sql = (
        f"COPY (SELECT {ACCT_COL_LIST} FROM account_snapshots "
        f"WHERE time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}' "
        f"ORDER BY time) TO STDOUT WITH CSV"
    )
    csv_data = remote_export_csv(export_sql)
    if not csv_data.strip():
        print("  ⏭️  远程无数据\n"); return
    rows = len(csv_data.strip().split("\n"))
    print(f"  📥 远程导出 {rows} 行")

    # We don't DELETE — account_snapshots has no PK, but we'd rather keep
    # existing rows and let imported ones be de-duped by time+account_id.
    # Since there's no unique constraint, we'll insert and trust the gap was empty.
    copy_cmd = f"\\COPY account_snapshots ({ACCT_COL_LIST}) FROM STDIN WITH CSV"
    ok, cnt = local_import_csv(copy_cmd, csv_data)
    print(f"  {'✅' if ok else '⚠️'} 导入: {cnt} 行" if ok else f"  ⚠️  导入异常: {cnt}")

    # Verify
    v = local_sql(
        f"SELECT count(*) FROM account_snapshots "
        f"WHERE time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}'"
    )
    rv = remote_sql(
        f"SELECT count(*) FROM account_snapshots "
        f"WHERE time >= '{ANALYSIS_START}' AND time < '{ANALYSIS_END}'"
    )
    ok = v == rv
    print(f"  {'✅' if ok else '⚠️'} 验证: 本地={v} 远程={rv}\n")


# ---- main ----

def main():
    parser = argparse.ArgumentParser(description="从远程 igzmf 补缺失数据")
    parser.add_argument("--dry-run", action="store_true", help="只查看概况")
    parser.add_argument("--force", action="store_true", help="跳过确认直接执行")
    parser.add_argument("--only", nargs="+", default=None, help="只补指定品种 (仅 ticks)")
    args = parser.parse_args()

    symbols = TICK_SYMBOLS
    if args.only:
        symbols = [s for s in TICK_SYMBOLS if s.upper() in [x.upper() for x in args.only]]
        if not symbols:
            print(f"❌ 未找到匹配品种: {args.only}"); sys.exit(1)

    print(f"\n🔍 分析时段: {ANALYSIS_START} ~ {ANALYSIS_END}")
    print(f"   模式: {'DRY RUN' if args.dry_run else '强制' if args.force else '交互确认'}\n")

    print("🔌 检查连接...")
    if local_sql("SELECT 1;") != "1":
        print("❌ 本地数据库连接失败！"); sys.exit(1)
    print("  ✅ 本地 OK")
    if remote_sql("SELECT 1;") != "1":
        print("❌ 远程 igzmf 连接失败！"); sys.exit(1)
    print("  ✅ 远程 igzmf OK")

    # 1) Ticks
    backfill_ticks(symbols, args.dry_run, args.force)

    # 2) Account snapshots
    backfill_account_snapshots(args.dry_run, args.force)

    print(f"\n{'='*50}")
    print("✅ 全部完成！\n")


if __name__ == "__main__":
    main()
