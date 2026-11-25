"""X Ads API から 4 階層の実績・CV タグ別・マスタを取得し、BigQuery の Source 層に書き込む（設計書 3 章）。

台帳のアカウントをループし、期間内に配信があったアカウントだけ、非同期レポートとマスタを取る。
1 つのアカウントが失敗しても他は続け、失敗があれば最後に 0 以外で終了する
"""
import json
import logging
import os
import sys
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from .config import settings
from .masters import Masters
from .stats import LEVELS, MAX_ENTITY_IDS, METRIC_GROUPS, chunks, expand, split_period, time_window, zone
from .x_client import NO_PERMISSION_CODES, XApiError, XClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

JST = ZoneInfo("Asia/Tokyo")
PLACEMENTS_DEFAULT = ["ALL_ON_TWITTER"]


@dataclass
class JobSpec:
    """1 本の非同期レポート（期間 × 階層 × 配信面 × ID 20 件 × 通常 / タグ別）"""
    period: Tuple[date, date]
    level: str
    placement: str
    ids: List[str]
    segmented: bool
    tz: ZoneInfo = JST
    attempts: int = 0
    job_id: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    @property
    def table(self) -> str:
        return f"x_ads_{self.level}_web_conversion_tag_stats" if self.segmented else f"x_ads_{self.level}_stats"


@dataclass
class AccountRun:
    account_id: str
    tz: ZoneInfo
    periods: List[Tuple[date, date]]
    active: Dict[Tuple[date, date], Dict[str, Dict[str, Set[str]]]] = field(default_factory=dict)


class Job:
    def __init__(self):
        self.run_id = uuid.uuid4().hex
        self.today = datetime.now(JST).date()
        self.client = XClient(settings.x_consumer_key, settings.x_consumer_secret, settings.x_access_token,
                              settings.x_access_token_secret, settings.x_ads_api_version, settings.request_timeout_seconds)
        self.writer = None
        if not settings.dry_run:
            from .bq_writer import BigQueryWriter
            self.writer = BigQueryWriter(settings.bq_project, settings.bq_dataset)
        self.failed = 0
        self._log_rows: List[Dict[str, Any]] = []
        self._log_lock = threading.Lock()
        self._active_accounts = 0
        self._staged: Dict[str, Dict[str, Any]] = {}

    # ---- 対象アカウント（台帳） ----
    def load_accounts(self) -> List[Dict[str, Any]]:
        from google.cloud import bigquery
        client = bigquery.Client(project=settings.bq_project)
        ids = [a.strip() for a in settings.account_ids.split(",") if a.strip()]
        sql = f"""
            SELECT account_id, account_name, status, time_zone
            FROM `{settings.accounts_table}`
            WHERE media = 'x' AND IFNULL(status, '') != 'DELETED'
              {"AND account_id IN UNNEST(@ids)" if ids else ""}
            ORDER BY account_id"""
        params = [bigquery.ArrayQueryParameter("ids", "STRING", ids)] if ids else []
        rows = [dict(r) for r in client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()]
        missing = set(ids) - {r["account_id"] for r in rows}
        if missing:
            logger.warning(f"台帳にないアカウント（取得しません）: {sorted(missing)}")
        return rows

    def period(self, tz: ZoneInfo) -> Tuple[date, date]:
        if settings.date_from and settings.date_to:
            return date.fromisoformat(settings.date_from), date.fromisoformat(settings.date_to)
        yesterday = datetime.now(tz).date() - timedelta(days=1)
        return yesterday - timedelta(days=settings.lookback_days - 1), yesterday

    # ---- 取得ログ ----
    def log(self, **row):
        row = {"run_id": self.run_id, "row_count": None, "error_message": None, "job_count": None, "level": None,
               "date_from": None, "date_to": None, **row}
        row["finished_at"] = datetime.now(timezone.utc).isoformat()
        level = logging.INFO if row["status"] in ("success", "skipped") else logging.WARNING
        logger.log(level, f"[{row.get('account_id')}] {row.get('target_table') or ''} {row['date_from'] or ''}〜{row['date_to'] or ''}: "
                          f"{row['status']} {row['row_count'] if row['row_count'] is not None else ''} {row['error_message'] or ''}".rstrip())
        with self._log_lock:
            if row["status"] == "failed":
                self.failed += 1
            self._log_rows.append(row)

    def flush_log(self):
        """取得ログはまとめて書く（配信のないアカウントが大半で、1 行ずつ書くと読み込みジョブが数百本になるため）"""
        with self._log_lock:
            rows, self._log_rows = self._log_rows, []
        if self.writer and rows:
            self.writer.append("x_ads_load_log", rows)

    def run(self) -> int:
        accounts = self.load_accounts()
        logger.info(f"run_id={self.run_id} アカウント {len(accounts)} 件 dry_run={settings.dry_run}")
        try:
            with ThreadPoolExecutor(max_workers=settings.max_parallel_accounts) as pool:
                list(pool.map(self.process_account, accounts))
            self.write_all()
        finally:
            self.flush_log()
        skipped = sum(1 for _ in accounts) - self._active_accounts
        logger.info(f"配信があったアカウント {self._active_accounts} 件 / 配信なし・権限なし {skipped} 件")
        if self.failed:
            logger.error(f"失敗が {self.failed} 件あります")
            return 1
        return 0

    # ---- 1 アカウント ----
    def process_account(self, account: Dict[str, Any]):
        account_id = account["account_id"]
        started = datetime.now(timezone.utc).isoformat()
        tz = zone(account.get("time_zone"))
        start, end = self.period(tz)
        run = AccountRun(account_id, tz, split_period(start, end, settings.max_days_per_job))
        base_log = {"account_id": account_id, "started_at": started, "date_from": start.isoformat(), "date_to": end.isoformat()}
        try:
            # 1. 期間内（と期間の前のアトリビューション期間）に配信があったエンティティ（設計書 3.2）
            for p in run.periods:
                run.active[p] = {lv: self.active_entities(account_id, tz, p, entity) for lv, entity in LEVELS.items()}
            if not any(ids for by_level in run.active.values() for by_pl in by_level.values() for ids in by_pl.values()):
                self.log(**base_log, target_table=None, status="skipped", error_message="期間内（と前のアトリビューション期間）に配信なし")
                return
            # 2. マスタ
            masters = Masters(self.client, account_id)
            masters.fetch_entities()
            active_ads = {i for by_level in run.active.values() for ids in by_level["promoted_tweet"].values() for i in ids}
            masters.fetch_tweets(active_ads)
        except XApiError as e:
            status = "skipped" if e.code in NO_PERMISSION_CODES else "failed"
            self.log(**base_log, target_table=None, status=status, error_message=str(e)[:1000])
            return
        except Exception as e:
            self.log(**base_log, target_table=None, status="failed", error_message=f"{type(e).__name__}: {str(e)[:1000]}")
            return

        with self._log_lock:
            self._active_accounts += 1
        # 3〜4. 非同期レポート
        specs = self.build_specs(run, masters.has_web_event_tags())
        logger.info(f"[{account_id}] 非同期レポート {len(specs)} 本（{start}〜{end}）")
        self.run_jobs(account_id, specs)

        # 5. 書き込み（実績はアカウント × 期間 × テーブルごと、マスタはアカウント × 取得日ごとに置き換え）
        meta = {"fetched_at": started, "run_id": self.run_id}
        parents = {"line_item": masters.campaign_of_line_item(), "promoted_tweet": masters.line_item_of_promoted_tweet()}
        for p in run.periods:
            for level in LEVELS:
                for segmented in (False, True):
                    group = [s for s in specs if s.period == p and s.level == level and s.segmented == segmented]
                    if segmented and not masters.has_web_event_tags():
                        continue
                    table = f"x_ads_{level}_web_conversion_tag_stats" if segmented else f"x_ads_{level}_stats"
                    log = {**base_log, "target_table": table, "level": level, "date_from": p[0].isoformat(),
                           "date_to": p[1].isoformat(), "job_count": len(group)}
                    errors = [s.error for s in group if s.error]
                    if errors:
                        # 一部のジョブが失敗した期間は書き込まない（前回の値を残す。翌日の取り直しで埋まる）
                        self.log(**log, status="failed", error_message=f"{len(errors)} 本失敗: {errors[0]}"[:1000])
                        continue
                    try:
                        self.stage(table, "date", self.to_rows(account_id, level, group, parents, meta), log)
                    except Exception as e:
                        self.log(**log, status="failed", error_message=f"{type(e).__name__}: {str(e)[:1000]}")

        snapshot_date = self.today
        for table, rows in masters.rows(snapshot_date.isoformat(), meta).items():
            log = {**base_log, "target_table": table, "date_from": snapshot_date.isoformat(), "date_to": snapshot_date.isoformat()}
            self.stage(table, "snapshot_date", rows, log)

    def active_entities(self, account_id: str, tz: ZoneInfo, p: Tuple[date, date], entity: str) -> Dict[str, Set[str]]:
        """配信面 → 期間の開始日の attribution_lookback_days 日前〜期間の終わりに配信があったエンティティ ID。
        配信を止めた後の CV だけの行も取るため、期間より前の配信も見る（active_entities も 45 日ずつで呼ぶ）"""
        by_placement: Dict[str, Set[str]] = defaultdict(set)
        start = p[0] - timedelta(days=settings.attribution_lookback_days)
        for q in split_period(start, p[1], settings.max_days_per_job):
            body = self.client.get(f"stats/accounts/{account_id}/active_entities", {"entity": entity, **time_window(tz, *q)})
            for row in body.get("data") or []:
                for placement in row.get("placements") or PLACEMENTS_DEFAULT:
                    by_placement[placement].add(str(row["entity_id"]))
        return dict(by_placement)

    def build_specs(self, run: AccountRun, with_tags: bool) -> List[JobSpec]:
        specs = []
        for p, by_level in run.active.items():
            for level, by_placement in by_level.items():
                for placement, ids in sorted(by_placement.items()):
                    for group in chunks(sorted(ids), MAX_ENTITY_IDS):
                        specs.append(JobSpec(p, level, placement, group, segmented=False, tz=run.tz))
                        if with_tags:
                            specs.append(JobSpec(p, level, placement, group, segmented=True, tz=run.tz))
        return specs

    def run_jobs(self, account_id: str, specs: List[JobSpec]):
        """非同期レポートを同時 max_concurrent_jobs 本まで作り、終わったものから結果を取る。失敗したジョブは 2 回まで作り直す"""
        queue = list(specs)
        pending: Dict[str, JobSpec] = {}
        deadline = time.time() + settings.job_timeout_seconds
        while queue or pending:
            while queue and len(pending) < settings.max_concurrent_jobs:
                spec = queue.pop(0)
                spec.attempts += 1
                try:
                    spec.job_id = self.create_job(account_id, spec)
                    pending[spec.job_id] = spec
                except XApiError as e:
                    spec.error = str(e)[:500]
            if not pending:
                continue
            if time.time() > deadline:
                for spec in pending.values():
                    spec.error = f"{settings.job_timeout_seconds} 秒待っても終わらなかった（job_id={spec.job_id}）"
                return
            time.sleep(settings.job_poll_seconds)
            for ids in chunks(list(pending), 200):
                try:
                    statuses = self.client.get(f"stats/jobs/accounts/{account_id}", {"job_ids": ",".join(ids)}).get("data") or []
                except XApiError as e:
                    logger.warning(f"[{account_id}] ジョブの状態を取得できませんでした: {e}")
                    continue
                for st in statuses:
                    spec = pending.get(str(st.get("id")))
                    if not spec:
                        continue
                    if st.get("status") == "SUCCESS" and st.get("url"):
                        del pending[spec.job_id]
                        try:
                            spec.result = self.client.download_json(st["url"])
                        except Exception as e:
                            spec.error = f"結果を取得できませんでした: {type(e).__name__}"
                    elif st.get("status") == "FAILED":
                        del pending[spec.job_id]
                        if spec.attempts < 3:
                            queue.append(spec)
                        else:
                            spec.error = f"非同期ジョブが失敗しました（job_id={spec.job_id}）"

    def create_job(self, account_id: str, spec: JobSpec) -> str:
        params = {"entity": LEVELS[spec.level], "entity_ids": ",".join(spec.ids), "granularity": "DAY",
                  "placement": spec.placement, **time_window(spec.tz, *spec.period)}
        if spec.segmented:
            params.update({"metric_groups": "WEB_CONVERSION", "segmentation_type": "CONVERSION_TAGS"})
        else:
            params["metric_groups"] = METRIC_GROUPS
        body = self.client.request("POST", f"stats/jobs/accounts/{account_id}", params)
        return str(body["data"]["id"])

    def to_rows(self, account_id: str, level: str, group: List[JobSpec], parents: Dict[str, Dict[str, str]],
                meta: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for spec in group:
            for entity_id, segment, day, metrics in expand(spec.result or {}, spec.period[0]):
                row: Dict[str, Any] = {"account_id": account_id, "date": day.isoformat(), "placement": spec.placement}
                if level == "campaign":
                    row["campaign_id"] = entity_id
                elif level == "line_item":
                    row.update(line_item_id=entity_id, campaign_id=parents["line_item"].get(entity_id))
                else:
                    li = parents["promoted_tweet"].get(entity_id)
                    row.update(promoted_tweet_id=entity_id, line_item_id=li, campaign_id=parents["line_item"].get(li) if li else None)
                if spec.segmented:
                    if not segment or not segment.get("segment_value"):
                        continue
                    row.update(web_event_tag_id=str(segment["segment_value"]), web_event_tag_name=segment.get("segment_name"))
                row.update(metrics=metrics, **meta)
                rows.append(row)
        return rows

    # ---- 書き込み（設計書 3.5） ----
    def stage(self, table: str, date_column: str, rows: List[Dict[str, Any]], log: Dict[str, Any]):
        """書き込む行をためる。全アカウントの取得が終わってから、テーブルごとに 1 回で置き換える
        （1 回の置き換えに約 10 秒かかり、アカウント × テーブルごとだと 700 回近くになるため）"""
        if settings.dump_dir:
            d = os.path.join(settings.dump_dir, log["account_id"])
            os.makedirs(d, exist_ok=True)
            with open(os.path.join(d, f"{table}.jsonl"), "a", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
        with self._log_lock:
            self._staged.setdefault(table, {"date_column": date_column, "entries": []})["entries"].append((rows, log))

    def write_all(self):
        """テーブルごとに、取得できた「アカウント × 期間」の行を消して、取った行を入れる（1 トランザクション）。
        別々のテーブルは並列で書く"""
        with ThreadPoolExecutor(max_workers=settings.max_parallel_accounts) as pool:
            list(pool.map(lambda item: self._write_table(*item), sorted(self._staged.items())))
        self._staged = {}

    def _write_table(self, table: str, staged: Dict[str, Any]):
        entries = staged["entries"]
        rows = [r for rs, _ in entries for r in rs]
        ranges = [(log["account_id"], log["date_from"], log["date_to"]) for _, log in entries]
        try:
            if self.writer:
                self.writer.replace_ranges(table, staged["date_column"], rows, ranges)
            for rs, log in entries:
                self.log(**log, status="success", row_count=len(rs))
            logger.info(f"{table}: {len(entries)} 件（アカウント × 期間）・{len(rows)} 行を書き込みました")
        except Exception as e:
            for _, log in entries:
                self.log(**log, status="failed", error_message=f"書き込みに失敗: {type(e).__name__}: {str(e)[:900]}")


def main() -> int:
    return Job().run()


if __name__ == "__main__":
    sys.exit(main())
