import os
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """環境変数（ローカルは .env、Cloud Run はジョブの環境変数と Secret Manager）から読む設定。"""

    # Cloud Run では Secret Manager の .env をファイルとしてマウントし、ENV_FILE でその場所を渡す
    model_config = SettingsConfigDict(env_file=os.environ.get("ENV_FILE", ".env"), env_file_encoding="utf-8", extra="ignore")

    # X Ads API（OAuth 1.0a）
    x_consumer_key: str = ""
    x_consumer_secret: str = ""
    x_access_token: str = ""
    x_access_token_secret: str = ""
    x_ads_api_version: str = "12"

    # 対象アカウントを引く台帳（媒体横断のアカウント台帳。dev / prd とも prd の台帳を使う）
    accounts_table: str = "days-ad-data-pipeline.intermediate_prd.int_ad_accounts__latest"
    # 取得するアカウント（カンマ区切り。空なら台帳の全アカウント）。ローカルで一部だけ試すとき用
    account_ids: str = ""

    # 昨日までの N 日を毎日取り直す（設計書 1.15）
    lookback_days: int = 30
    # バックフィル・取り直し用。両方指定するとルックバックの代わりにこの期間を取る（YYYY-MM-DD）
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    # 1 回の非同期レポートの最大日数。タグ別（セグメント）は 45 日までなので、通常のレポートも同じ区切りで取る
    max_days_per_job: int = 45

    # アカウントごとに同時に処理中にする非同期ジョブの数（X の上限は 100）
    max_concurrent_jobs: int = 80
    # 非同期ジョブの状態を確かめる間隔と、1 アカウントで待つ最大時間
    job_poll_seconds: int = 10
    job_timeout_seconds: int = 1800

    # 並列で処理するアカウント数（BigQuery への書き込みはテーブルごとに 1 つずつ）
    max_parallel_accounts: int = 4
    request_timeout_seconds: int = 60

    # 出力先
    bq_project: str = "days-ad-data-pipeline"
    bq_dataset: str = "source_dev"
    # true なら BigQuery に書かず、取得した行数だけ出力する
    dry_run: bool = False
    # 指定すると、取得した行をアカウント × テーブルごとに JSON Lines で保存する（確認用）
    dump_dir: Optional[str] = None


settings = Settings()
