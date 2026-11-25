# x-ads-loader

X 広告（X Ads API v12）から、キャンペーン・広告グループ（line item）・広告（promoted tweet）の日別実績と、Web の CV のタグ別の実績、マスタを毎日取得し、BigQuery の Source 層（`source_{env}.x_ads_*`）に書き込む Cloud Run ジョブです。

設計の全体は days-ad-data-pipeline の `docs/X_ADS_DESIGN.md` を参照してください（取得仕様は 3 章、テーブル定義は 5.1）。Staging 以降は days-ad-data-pipeline の Dataform で作ります。

## 書き込むテーブル

| テーブル | 粒度 | 内容 |
| :--- | :--- | :--- |
| `x_ads_{campaign,line_item,promoted_tweet}_stats` | 各階層 × 日付 × 配信面 | 実績。指標は `metrics`（JSON）にその日の値だけを入れる（CV はオブジェクトのまま） |
| `x_ads_{campaign,line_item,promoted_tweet}_web_conversion_tag_stats` | 上 × Web の CV タグ | Web の CV のタグ別（`segmentation_type=CONVERSION_TAGS`） |
| `x_ads_{campaign,line_item,promoted_tweet}_snapshots` | エンティティ × 取得日 | マスタ（削除済みも含む全件） |
| `x_ads_tweet_snapshots` | 投稿 × 取得日 | 配信があった広告の投稿の名前・本文・カード・メディアのファイル名（広告名の元） |
| `x_ads_web_event_tag_snapshots` / `x_ads_app_event_tag_snapshots` | タグ・イベント × 取得日 | Web の CV タグ・アプリのイベント（アトリビューション期間、Adjust のイベント名） |
| `x_ads_load_log` | 実行 × アカウント × テーブル × 期間 | 取得ログ（success / failed / skipped） |

実績は `date`、マスタは `snapshot_date` で日付パーティション、`account_id` でクラスタリングします。

## 処理の流れ

1. 台帳（`intermediate_prd.int_ad_accounts__latest` の `media = 'x'`、削除済みを除く）からアカウントを引く
2. アカウントごとに（既定 4 並列）
   1. `active_entities` で、期間の開始日の 30 日前〜期間の終わりに配信があったキャンペーン・広告グループ・広告と配信面を取る。どれもなければ `skipped`（大半のアカウントがこれに当たる）。期間より前も見るのは、CV が「CV が起きた日」に付き、配信を止めた後もアトリビューション期間（最長 30 日）のあいだ付くため（見ないと、その CV だけの行が置き換えで消える）
   2. マスタを取る（キャンペーン・広告グループ・広告・Web の CV タグ・アプリのイベントは全件、投稿・カード・メディアは配信があった広告の分だけ）
   3. 非同期レポート（`POST stats/jobs`）を、期間（45 日ずつ）× 階層 × 配信面 × ID 20 件ずつ作る。通常版（6 指標グループ）とタグ別版の 2 本。同時に処理中のジョブはアカウントごとに 80 本まで
   4. 10 秒ごとに状態を確かめ、終わったものから結果（gzip の JSON）を取って、エンティティ × 日付の 1 行に展開する。その日の指標がすべて null か 0 の行は作らない
3. 全アカウントの取得が終わったら、テーブルごとに 1 回、「取得できたアカウント × 期間」の行を消して取った行を入れる（1 トランザクション。別々のテーブルは並列）
4. 失敗は `x_ads_load_log` に `failed` で残し、他は続ける。失敗が 1 件でもあれば最後に失敗で終了する。翌日に 30 日分を取り直すので、一時的な失敗は自然に埋まる

* 一部の非同期ジョブが失敗した「アカウント × 期間 × テーブル」は書き込まない（前回の値が残る）
* 権限のないアカウント（`UNAUTHORIZED_ACCESS` / `NOT_FOUND`）は `skipped`

## 環境変数

| 変数 | 既定 | 内容 |
| :--- | :--- | :--- |
| `X_CONSUMER_KEY` / `X_CONSUMER_SECRET` / `X_ACCESS_TOKEN` / `X_ACCESS_TOKEN_SECRET` | — | X Ads API の認証情報（OAuth 1.0a。ad-account-registry・ad-monitor と同じ） |
| `ACCOUNTS_TABLE` | `days-ad-data-pipeline.intermediate_prd.int_ad_accounts__latest` | 対象アカウントを引く台帳 |
| `ACCOUNT_IDS` | （全アカウント） | 取得するアカウント（カンマ区切り）。ローカルで一部だけ試すとき用 |
| `LOOKBACK_DAYS` | `30` | 昨日までの N 日を毎日取り直す |
| `DATE_FROM` / `DATE_TO` | — | バックフィル・取り直し用。両方指定するとこの期間を取る |
| `MAX_DAYS_PER_JOB` | `45` | 1 本の非同期レポートの最大日数（タグ別の上限が 45 日） |
| `ATTRIBUTION_LOOKBACK_DAYS` | `30` | 配信の有無（`active_entities`）を、期間の開始日の何日前から見るか |
| `MAX_CONCURRENT_JOBS` | `80` | アカウントごとに同時に処理中にするジョブの数（X の上限は 100） |
| `MAX_PARALLEL_ACCOUNTS` | `4` | 並列で処理するアカウント数 |
| `BQ_PROJECT` / `BQ_DATASET` | `days-ad-data-pipeline` / `source_dev` | 出力先 |
| `DRY_RUN` | `false` | `true` なら BigQuery に書かない |
| `DUMP_DIR` | — | 指定すると、書き込む行をアカウント × テーブルごとに JSON Lines で保存する（確認用） |
| `ENV_FILE` | `.env` | 設定と認証情報を読む .env の場所（Cloud Run では Secret Manager をマウントした場所） |

## ローカルでの実行

```bash
cp .env.example .env   # X_* の認証情報を入れる
uv venv .venv && uv pip install -p .venv -r requirements.txt
gcloud auth application-default login   # 台帳の読み取り・source_dev への書き込みに使う

# BigQuery に書かずに、1 アカウント・1 週間分を取り、行をファイルに保存する
DRY_RUN=true ACCOUNT_IDS=18ce55wu9vz DATE_FROM=2026-11-10 DATE_TO=2026-11-16 DUMP_DIR=/tmp/xads .venv/bin/python -m src.main

# source_dev に書き込む
DRY_RUN=false .venv/bin/python -m src.main
```

## prd へのデプロイ

```bash
gcloud auth login && gcloud auth application-default login
./scripts/deploy_prd.sh   # 何度実行してもよい（コードを変えたらこれで再デプロイ）
```

`scripts/deploy_prd.sh` が次のものを作ります（手順は adjust-loader・ad-account-registry と同じ）。

| 種類 | 名前 | 内容 |
| :--- | :--- | :--- |
| サービスアカウント | `x-ads-loader` / `x-ads-loader-invoker` | ジョブの実行用 / Workflows・Scheduler 用 |
| Secret Manager | `x-ads-loader-env` | `.env` のうち `X_*` の認証情報だけ。ジョブに `/secrets/env/.env` としてマウント |
| Cloud Run ジョブ | `x-ads-loader-prd` | `source_prd` に書き込む。台帳（`intermediate_prd`）は読み取りだけ。タイムアウト 3 時間 |
| Workflows | `invoke-x-ads-loader-prd` | ジョブを実行して完了まで待ち、失敗したら Slack に通知 |
| Cloud Scheduler | `x-ads-loader-daily` | 毎日 5:30 JST に Workflows を起動 |

バックフィル・取り直しは、Workflows を引数付きで実行します: `gcloud workflows run invoke-x-ads-loader-prd --location=us-central1 --data='{"date_from": "2026-10-01", "date_to": "2026-11-19"}'`
