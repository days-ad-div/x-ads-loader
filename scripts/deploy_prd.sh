#!/usr/bin/env bash
# X 広告の取得ジョブを prd にデプロイする（サービスアカウント → Secret Manager → 権限 → Cloud Run ジョブ → Workflows → Scheduler）。
# 手順は ad-account-registry の scripts/deploy_prd.sh に合わせている。
# 何度実行してもよい（作成済みのものはそのまま、ジョブ・Workflows は上書きデプロイ）。
# 認証情報の値は表示しない。
set -euo pipefail

P=days-ad-data-pipeline
REGION=us-central1
JOB=x-ads-loader-prd
WORKFLOW=invoke-x-ads-loader-prd
SCHEDULER=x-ads-loader-daily
SECRET=x-ads-loader-env
JOB_SA_NAME=x-ads-loader
INVOKER_SA_NAME=x-ads-loader-invoker
JOB_SA=$JOB_SA_NAME@$P.iam.gserviceaccount.com
INVOKER_SA=$INVOKER_SA_NAME@$P.iam.gserviceaccount.com

cd "$(dirname "$0")/.."

echo "== 0. サービスアカウント（ジョブ用・起動用）"
for pair in "$JOB_SA_NAME:X 広告取得ジョブ" "$INVOKER_SA_NAME:X 広告取得ジョブの起動（Workflows・Scheduler）"; do
  name=${pair%%:*}; display=${pair#*:}
  if gcloud iam service-accounts describe $name@$P.iam.gserviceaccount.com --project=$P >/dev/null 2>&1; then
    echo "   $name: 作成済み"
  else
    gcloud iam service-accounts create $name --project=$P --display-name="$display"
  fi
done

echo "== 1. Secret Manager: $SECRET（.env から X Ads API の認証情報だけを抜き出す）"
ENV_KEYS='^X_(CONSUMER_KEY|CONSUMER_SECRET|ACCESS_TOKEN|ACCESS_TOKEN_SECRET)='
if gcloud secrets describe $SECRET --project=$P >/dev/null 2>&1; then
  echo "   作成済みのためスキップ（値を入れ替えるときは: grep -E '$ENV_KEYS' .env | gcloud secrets versions add $SECRET --project=$P --data-file=-）"
else
  grep -E "$ENV_KEYS" .env | gcloud secrets create $SECRET --project=$P \
    --replication-policy=automatic --labels=app=x-ads-loader --data-file=-
fi
echo "   登録されている変数名: $(gcloud secrets versions access latest --secret=$SECRET --project=$P | grep -oE '^[A-Z_]+' | tr '\n' ' ')"

echo "== 2. ジョブ用の権限（source_prd の編集・台帳の読み取り・クエリの実行・認証情報の読み取り）"
# データセット単位の権限は bq add-iam-policy-binding（テーブル・ビュー用）では付けられないため、DCL で付ける
bq query --project_id=$P --nouse_legacy_sql --quiet \
  "GRANT \`roles/bigquery.dataEditor\` ON SCHEMA \`$P.source_prd\` TO \"serviceAccount:$JOB_SA\"" >/dev/null
# 対象アカウントは台帳（intermediate_prd.int_ad_accounts__latest）から引く
bq query --project_id=$P --nouse_legacy_sql --quiet \
  "GRANT \`roles/bigquery.dataViewer\` ON SCHEMA \`$P.intermediate_prd\` TO \"serviceAccount:$JOB_SA\"" >/dev/null
gcloud projects add-iam-policy-binding $P --member=serviceAccount:$JOB_SA --role=roles/bigquery.jobUser --condition=None --format=none
gcloud secrets add-iam-policy-binding $SECRET --project=$P --member=serviceAccount:$JOB_SA --role=roles/secretmanager.secretAccessor --format=none

echo "== 3. Cloud Run ジョブ: $JOB（ソースからビルド。.env は .gcloudignore で除外）"
# dev の実測: 全 394 アカウント・10/1〜11/19 のバックフィルで約 7 分（配信ありは 38 アカウント）。毎日の実行も同程度の見込み。バックフィル（DATE_FROM / DATE_TO）に備えてタイムアウトは 3 時間
gcloud run jobs deploy $JOB --project=$P --region=$REGION --source=. \
  --service-account=$JOB_SA \
  --set-env-vars=BQ_DATASET=source_prd,DRY_RUN=false,ENV_FILE=/secrets/env/.env \
  --set-secrets=/secrets/env/.env=$SECRET:latest \
  --cpu=1 --memory=1Gi --max-retries=0 --task-timeout=3h \
  --labels=app=x-ads-loader

echo "== 4. Workflows・Scheduler 用の権限（ジョブの実行・Slack の Webhook の読み取り・Workflows の起動）"
gcloud run jobs add-iam-policy-binding $JOB --project=$P --region=$REGION --member=serviceAccount:$INVOKER_SA --role=roles/run.developer --format=none
# Workflows のコネクタはジョブの完了を operation で待つ。run.operations.get はジョブ単位では付けられないため、
# プロジェクト単位の読み取り専用ロールを付ける
gcloud projects add-iam-policy-binding $P --member=serviceAccount:$INVOKER_SA --role=roles/run.viewer --condition=None --format=none
gcloud secrets add-iam-policy-binding SLACK_WEBHOOK_URL --project=$P --member=serviceAccount:$INVOKER_SA --role=roles/secretmanager.secretAccessor --format=none
gcloud projects add-iam-policy-binding $P --member=serviceAccount:$INVOKER_SA --role=roles/workflows.invoker --condition=None --format=none

echo "== 5. Workflows: $WORKFLOW"
gcloud workflows deploy $WORKFLOW --project=$P --location=$REGION \
  --source=workflows/invoke_x_ads_loader_prd.yaml --service-account=$INVOKER_SA \
  --labels=app=x-ads-loader

SCHEDULE="30 5 * * *"
echo "== 6. Cloud Scheduler: $SCHEDULER（毎日 5:30 JST。Adjust（5:00）の後、7:30 ごろの Dataform の前）"
if gcloud scheduler jobs describe $SCHEDULER --project=$P --location=$REGION >/dev/null 2>&1; then
  echo "   作成済みのため、スケジュールだけ更新"
  gcloud scheduler jobs update http $SCHEDULER --project=$P --location=$REGION \
    --schedule="$SCHEDULE" --time-zone="Asia/Tokyo" --format=none
else
  gcloud scheduler jobs create http $SCHEDULER --project=$P --location=$REGION \
    --schedule="$SCHEDULE" --time-zone="Asia/Tokyo" \
    --uri="https://workflowexecutions.googleapis.com/v1/projects/$P/locations/$REGION/workflows/$WORKFLOW/executions" \
    --http-method=POST --message-body='{"argument": "{}"}' \
    --oauth-service-account-email=$INVOKER_SA \
    --max-retry-attempts=0
fi

echo "== 完了"
