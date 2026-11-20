"""X Ads API のクライアント（OAuth 1.0a）。ad-account-registry の XAdsSource の要求処理を元にしている"""
import gzip
import json
import logging
import time
from typing import Any, Dict, List, Optional

import requests
from requests_oauthlib import OAuth1

logger = logging.getLogger(__name__)

BASE_URL = "https://ads-api.x.com"

# 権限がないアカウントのエラーコード（台帳ジョブと同じ扱い）
NO_PERMISSION_CODES = {"UNAUTHORIZED_ACCESS", "NOT_FOUND"}


class XApiError(Exception):
    """Ads API がエラーを返した場合の例外（メッセージにトークンを含めない）"""

    def __init__(self, message: str, code: Optional[str] = None, status: Optional[int] = None):
        super().__init__(message)
        self.code = code
        self.status = status


class XClient:
    def __init__(self, consumer_key: str, consumer_secret: str, access_token: str, access_token_secret: str,
                 version: str = "12", timeout: int = 60):
        if not all([consumer_key, consumer_secret, access_token, access_token_secret]):
            raise SystemExit("X_CONSUMER_KEY / X_CONSUMER_SECRET / X_ACCESS_TOKEN / X_ACCESS_TOKEN_SECRET を設定してください")
        self.base_url = f"{BASE_URL}/{version}"
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = OAuth1(consumer_key, consumer_secret, access_token, access_token_secret)
        self._download = requests.Session()  # 結果の URL は署名なしで取る

    def request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """レスポンス全体を返す。レート制限（429）はリセット時刻まで、5xx は指数バックオフで待って最大 3 回リトライ"""
        for attempt in range(4):
            try:
                res = self.session.request(method, f"{self.base_url}/{path}", params=params, timeout=self.timeout)
            except requests.RequestException as e:
                if attempt == 3:
                    raise XApiError(f"{path}: {type(e).__name__}") from None
                time.sleep(2 ** (attempt + 1))
                continue
            if res.status_code == 429 and attempt < 3:
                reset = int(res.headers.get("x-rate-limit-reset", 0) or 0)
                wait = min(max(reset - time.time(), 1), 960) if reset else 2 ** (attempt + 1)
                logger.warning(f"レート制限に達したため {int(wait)} 秒待ちます（{path}）")
                time.sleep(wait)
                continue
            if res.status_code >= 500 and attempt < 3:
                time.sleep(2 ** (attempt + 1))
                continue
            try:
                body = res.json() if res.content else {}
            except ValueError:
                body = {}
            if res.status_code in (200, 201):
                return body
            err = (body.get("errors") or [{}])[0]
            raise XApiError(f"{path}: HTTP {res.status_code} code={err.get('code')} {err.get('message', res.reason)}",
                            err.get("code"), res.status_code)
        return {}

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.request("GET", path, params)

    def get_list(self, path: str, params: Optional[Dict[str, Any]] = None, count: int = 1000) -> List[Dict[str, Any]]:
        """next_cursor を辿って data を全件結合して返す"""
        items: List[Dict[str, Any]] = []
        cursor = None
        while True:
            p = {**(params or {}), **({"count": count} if count else {}), **({"cursor": cursor} if cursor else {})}
            body = self.get(path, p)
            data = body.get("data") or []
            items.extend(data if isinstance(data, list) else [data])
            cursor = body.get("next_cursor")
            if not cursor:
                return items

    def download_json(self, url: str) -> Dict[str, Any]:
        """非同期レポートの結果（gzip の JSON）を取る"""
        for attempt in range(4):
            try:
                res = self._download.get(url, timeout=self.timeout * 2)
                res.raise_for_status()
                raw = res.content
                try:
                    raw = gzip.decompress(raw)
                except OSError:
                    pass  # すでに展開済みで返る場合
                return json.loads(raw)
            except (requests.RequestException, ValueError):
                if attempt == 3:
                    raise
                time.sleep(2 ** (attempt + 1))
        return {}
