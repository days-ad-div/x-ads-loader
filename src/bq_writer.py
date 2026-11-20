"""BigQuery の Source 層への書き込み（設計書 3.5・5.1）。adjust-loader の bq_writer と同じ方式"""
import logging
import threading
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

logger = logging.getLogger(__name__)
F = bigquery.SchemaField

COMMON = [
    F("account_id", "STRING", mode="REQUIRED", description="広告アカウント ID"),
]
META = [
    F("fetched_at", "TIMESTAMP", mode="REQUIRED", description="取得日時"),
    F("run_id", "STRING", mode="REQUIRED", description="取得ジョブの実行 ID"),
]
METRICS = F("metrics", "JSON", description="その日の指標（API の項目名のまま。CV はオブジェクトのまま、その日の値だけ。null の項目は省く）")
STATS_KEYS = [
    F("date", "DATE", mode="REQUIRED", description="日付（アカウントのタイムゾーン）"),
    F("placement", "STRING", mode="REQUIRED", description="配信面（ALL_ON_TWITTER / PUBLISHER_NETWORK）"),
]
TAG_KEYS = [
    F("web_event_tag_id", "STRING", mode="REQUIRED", description="Web の CV タグ ID（segment_value）"),
    F("web_event_tag_name", "STRING", description="Web の CV タグ名（segment_name）"),
]

LEVEL_IDS = {
    "campaign": [F("campaign_id", "STRING", mode="REQUIRED", description="キャンペーン ID")],
    "line_item": [F("campaign_id", "STRING", description="キャンペーン ID（親）"),
                  F("line_item_id", "STRING", mode="REQUIRED", description="広告グループ（line item）ID")],
    "promoted_tweet": [F("campaign_id", "STRING", description="キャンペーン ID（親）"),
                       F("line_item_id", "STRING", description="広告グループ ID（親）"),
                       F("promoted_tweet_id", "STRING", mode="REQUIRED", description="広告（promoted tweet）ID")],
}

SCHEMAS: Dict[str, List[bigquery.SchemaField]] = {}
for _level, _ids in LEVEL_IDS.items():
    SCHEMAS[f"x_ads_{_level}_stats"] = COMMON + _ids + STATS_KEYS + [METRICS] + META
    SCHEMAS[f"x_ads_{_level}_web_conversion_tag_stats"] = COMMON + _ids + STATS_KEYS + TAG_KEYS + [METRICS] + META

SNAP = [F("snapshot_date", "DATE", mode="REQUIRED", description="取得日（JST）")]
TS = lambda name, desc=None: F(name, "TIMESTAMP", description=desc)  # noqa: E731
S = lambda name, desc=None: F(name, "STRING", description=desc)  # noqa: E731
I = lambda name, desc=None: F(name, "INT64", description=desc)  # noqa: E731
B = lambda name, desc=None: F(name, "BOOL", description=desc)  # noqa: E731
RAW = F("raw", "JSON", description="API の 1 件そのまま")
ENTITY_TAIL = [B("deleted", "削除済みか"), TS("created_at"), TS("updated_at", "媒体側の更新日時"), RAW]

SCHEMAS["x_ads_campaign_snapshots"] = COMMON + SNAP + [
    F("campaign_id", "STRING", mode="REQUIRED"), S("campaign_name"),
    S("entity_status", "配信状態（ACTIVE / PAUSED など）"), S("effective_status", "実際の配信状態"), B("servable"),
    I("daily_budget_amount_local_micro", "日予算（マイクロ単位）"), I("total_budget_amount_local_micro", "総予算（マイクロ単位）"),
    S("budget_optimization"), S("funding_instrument_id"), S("currency"),
] + ENTITY_TAIL + META
SCHEMAS["x_ads_line_item_snapshots"] = COMMON + SNAP + [
    F("line_item_id", "STRING", mode="REQUIRED"), S("line_item_name"), S("campaign_id"),
    S("objective", "目的"), S("goal", "最適化の目標"), S("bid_strategy"),
    I("bid_amount_local_micro", "入札単価（マイクロ単位）"), I("target_cpa_local_micro", "目標 CPA（マイクロ単位）"),
    S("pay_by", "課金方式"), F("placements", "STRING", mode="REPEATED", description="配信面"), S("product_type"),
    S("primary_web_event_tag", "最適化の目標にしている Web の CV タグ"),
    S("ios_app_store_identifier"), S("android_app_store_identifier"),
    TS("start_time"), TS("end_time"), S("entity_status"), S("currency"),
] + ENTITY_TAIL + META
SCHEMAS["x_ads_promoted_tweet_snapshots"] = COMMON + SNAP + [
    F("promoted_tweet_id", "STRING", mode="REQUIRED"), S("line_item_id"), S("tweet_id"),
    S("approval_status", "審査状態"), S("entity_status"),
] + ENTITY_TAIL + META
SCHEMAS["x_ads_tweet_snapshots"] = COMMON + SNAP + [
    F("tweet_id", "STRING", mode="REQUIRED"), S("tweet_name", "入稿時に付けた投稿の名前（管理画面の広告名）"),
    S("full_text", "本文"), S("card_uri"), S("card_name"), S("card_type"),
    F("media_keys", "STRING", mode="REPEATED", description="投稿・カードで使っているメディア"),
    F("media_names", "STRING", mode="REPEATED", description="メディアライブラリの名前（media_keys と同じ順）"),
    F("media_file_names", "STRING", mode="REPEATED", description="メディアのファイル名（media_keys と同じ順）"),
    TS("tweet_created_at"), F("raw", "JSON", description="投稿・カード・メディアの応答をまとめたもの"),
] + META
SCHEMAS["x_ads_web_event_tag_snapshots"] = COMMON + SNAP + [
    F("web_event_tag_id", "STRING", mode="REQUIRED"), S("web_event_tag_name"), S("type", "CV の種類"),
    S("status", "計測状態（TRACKING / UNVERIFIED など）"),
    I("click_window", "エンゲージ後のアトリビューション期間（日）"), I("view_through_window", "表示後のアトリビューション期間（日）"),
    B("retargeting_enabled"), TS("last_tracked_at"),
] + ENTITY_TAIL + META
SCHEMAS["x_ads_app_event_tag_snapshots"] = COMMON + SNAP + [
    F("app_event_tag_id", "STRING", mode="REQUIRED"), S("conversion_type", "X の CV の種類（INSTALL / PURCHASE など）"),
    S("os_type"), S("app_store_identifier"), S("provider_name"), S("provider_app_event_id"),
    S("provider_app_event_name", "計測ツール（Adjust など）のイベント名"),
    I("post_engagement_attribution_window", "エンゲージ後のアトリビューション期間（日）"),
    I("post_view_attribution_window", "表示後のアトリビューション期間（日）"),
    B("retargeting_enabled"), S("status"), TS("last_tracked_at"),
] + ENTITY_TAIL + META
SCHEMAS["x_ads_load_log"] = [
    F("run_id", "STRING", mode="REQUIRED"), S("account_id"), S("target_table"), S("level"),
    F("date_from", "DATE"), F("date_to", "DATE"), I("job_count", "非同期ジョブの数"),
    F("status", "STRING", mode="REQUIRED", description="success / failed / skipped"),
    I("row_count"), S("error_message"),
    F("started_at", "TIMESTAMP", mode="REQUIRED"), F("finished_at", "TIMESTAMP", mode="REQUIRED"),
]

PARTITIONS = {t: ("snapshot_date" if t.endswith("_snapshots") else "date") for t in SCHEMAS if t != "x_ads_load_log"}
PARTITIONS["x_ads_load_log"] = "started_at"
CLUSTERING = {t: ["account_id"] for t in SCHEMAS if t != "x_ads_load_log"}


class BigQueryWriter:
    def __init__(self, project: str, dataset: str):
        self.client = bigquery.Client(project=project)
        self.dataset = f"{project}.{dataset}"
        self._ensured: set = set()
        # 同じテーブルの日付パーティションを複数のアカウントが共有するため、書き込みはテーブルごとに 1 つずつにする
        self._locks = {table: threading.Lock() for table in SCHEMAS}
        self._ensure_lock = threading.Lock()

    def ensure_table(self, table: str):
        with self._ensure_lock:
            if table in self._ensured:
                return
            t = bigquery.Table(f"{self.dataset}.{table}", schema=SCHEMAS[table])
            t.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field=PARTITIONS[table])
            if table in CLUSTERING:
                t.clustering_fields = CLUSTERING[table]
            self.client.create_table(t, exists_ok=True)
            self._ensured.add(table)

    def replace(self, table: str, rows: List[Dict[str, Any]], where: str, params: List[Any]) -> int:
        """本テーブルの where に当たる行を消し、rows を入れる（1 トランザクション）。rows が空なら消すだけ"""
        self.ensure_table(table)
        columns = ", ".join(f"`{f.name}`" for f in SCHEMAS[table])
        temp = None
        if rows:
            temp = f"{self.dataset}.LOAD_TEMP_{table}_{uuid.uuid4().hex[:12]}"
            self._load(temp, table, rows)
        sql = f"BEGIN TRANSACTION;\nDELETE FROM `{self.dataset}.{table}` WHERE {where};\n"
        if temp:
            sql += f"INSERT INTO `{self.dataset}.{table}` ({columns}) SELECT {columns} FROM `{temp}`;\n"
        sql += "COMMIT TRANSACTION;"
        try:
            with self._locks[table]:
                self._run_with_retry(sql, params)
        finally:
            if temp:
                self.client.delete_table(temp, not_found_ok=True)
        return len(rows)

    def replace_ranges(self, table: str, date_column: str, rows: List[Dict[str, Any]], ranges) -> int:
        """複数の「アカウント × 期間」をまとめて置き換える（1 トランザクション）。

        ranges: [(account_id, date_from, date_to)]。この範囲の行を消し、rows を入れる
        """
        if not ranges:
            return 0
        self.ensure_table(table)
        columns = ", ".join(f"`{f.name}`" for f in SCHEMAS[table])
        temp = None
        if rows:
            temp = f"{self.dataset}.LOAD_TEMP_{table}_{uuid.uuid4().hex[:12]}"
            self._load(temp, table, rows)
        sql = (f"BEGIN TRANSACTION;\n"
               f"DELETE FROM `{self.dataset}.{table}` t\n"
               f"WHERE t.`{date_column}` BETWEEN @min_date AND @max_date\n"
               f"  AND EXISTS (SELECT 1 FROM UNNEST(@ranges) r\n"
               f"              WHERE r.account_id = t.account_id AND t.`{date_column}` BETWEEN r.date_from AND r.date_to);\n")
        if temp:
            sql += f"INSERT INTO `{self.dataset}.{table}` ({columns}) SELECT {columns} FROM `{temp}`;\n"
        sql += "COMMIT TRANSACTION;"
        params = [_ranges_param(ranges),
                  date_param("min_date", min(_date(f) for _, f, _ in ranges)),
                  date_param("max_date", max(_date(t) for _, _, t in ranges))]
        try:
            with self._locks[table]:
                self._run_with_retry(sql, params)
        finally:
            if temp:
                self.client.delete_table(temp, not_found_ok=True)
        return len(rows)


    def append(self, table: str, rows: List[Dict[str, Any]]):
        if not rows:
            return
        self.ensure_table(table)
        job_config = bigquery.LoadJobConfig(schema=SCHEMAS[table], write_disposition="WRITE_APPEND",
                                            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
        self.client.load_table_from_json(rows, f"{self.dataset}.{table}", job_config=job_config).result()

    def _load(self, temp: str, table: str, rows: List[Dict[str, Any]]):
        t = bigquery.Table(temp, schema=SCHEMAS[table])
        t.expires = datetime.now(timezone.utc) + timedelta(hours=1)  # 失敗しても 1 時間で消える
        self.client.create_table(t)
        job_config = bigquery.LoadJobConfig(schema=SCHEMAS[table], write_disposition="WRITE_APPEND",
                                            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
        self.client.load_table_from_json(rows, temp, job_config=job_config).result()

    def _run_with_retry(self, sql: str, params: List[Any], retries: int = 3):
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        for attempt in range(retries + 1):
            try:
                self.client.query(sql, job_config=job_config).result()
                return
            except BadRequest as e:
                # 別のジョブ（Dataform など）と同時に更新すると衝突することがある
                if "concurrent update" not in str(e).lower() or attempt == retries:
                    raise
                wait = 5 * (attempt + 1)
                logger.warning(f"トランザクションが衝突したため {wait} 秒後に再試行します: {str(e)[:200]}")
                time.sleep(wait)


def date_param(name: str, value) -> bigquery.ScalarQueryParameter:
    return bigquery.ScalarQueryParameter(name, "DATE", value)


def string_param(name: str, value: Optional[str]) -> bigquery.ScalarQueryParameter:
    return bigquery.ScalarQueryParameter(name, "STRING", value)



def _ranges_param(ranges) -> bigquery.ArrayQueryParameter:
    return bigquery.ArrayQueryParameter("ranges", "STRUCT", [
        bigquery.StructQueryParameter(None,
                                      bigquery.ScalarQueryParameter("account_id", "STRING", a),
                                      bigquery.ScalarQueryParameter("date_from", "DATE", _date(f)),
                                      bigquery.ScalarQueryParameter("date_to", "DATE", _date(t)))
        for a, f, t in ranges])



def _date(v):
    return date.fromisoformat(v) if isinstance(v, str) else v
