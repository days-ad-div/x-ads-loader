"""マスタ（キャンペーン・広告グループ・広告・投稿・CV タグ・アプリのイベント）のスナップショット（設計書 1.11・1.12・1.13）"""
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from .stats import chunks
from .x_client import XApiError, XClient

logger = logging.getLogger(__name__)

# 1 回で指定できる投稿・カードの数
MAX_TWEET_IDS = 200
MAX_CARD_URIS = 100


def _s(v: Any) -> Optional[str]:
    return None if v is None else str(v)


def _i(v: Any) -> Optional[int]:
    try:
        return None if v is None else int(v)
    except (TypeError, ValueError):
        return None


def _tweet_time(v: Optional[str]) -> Optional[str]:
    """投稿の created_at（例: Wed Nov 04 05:04:14 +0000 2026）を ISO 形式にする"""
    if not v:
        return None
    try:
        return datetime.strptime(v, "%a %b %d %H:%M:%S %z %Y").isoformat()
    except ValueError:
        return v


def _tail(row: Dict[str, Any]) -> Dict[str, Any]:
    return {"deleted": row.get("deleted"), "created_at": row.get("created_at"), "updated_at": row.get("updated_at"), "raw": row}


class Masters:
    """1 アカウントのマスタを取り、スナップショットの行にする"""

    def __init__(self, client: XClient, account_id: str):
        self.client = client
        self.account_id = account_id
        self.campaigns: List[Dict[str, Any]] = []
        self.line_items: List[Dict[str, Any]] = []
        self.promoted_tweets: List[Dict[str, Any]] = []
        self.web_event_tags: List[Dict[str, Any]] = []
        self.app_event_tags: List[Dict[str, Any]] = []
        self.tweets: List[Dict[str, Any]] = []

    def _list(self, name: str) -> List[Dict[str, Any]]:
        return self.client.get_list(f"accounts/{self.account_id}/{name}", {"with_deleted": "true"})

    def fetch_entities(self):
        self.campaigns = self._list("campaigns")
        self.line_items = self._list("line_items")
        self.promoted_tweets = self._list("promoted_tweets")
        self.web_event_tags = self._list("web_event_tags")
        self.app_event_tags = self._list("app_event_tags")

    # 親の ID（レポートの結果にはエンティティ自身の ID しかないため、マスタから付ける。設計書 3.3）
    def campaign_of_line_item(self) -> Dict[str, str]:
        return {str(r["id"]): _s(r.get("campaign_id")) for r in self.line_items}

    def line_item_of_promoted_tweet(self) -> Dict[str, str]:
        return {str(r["id"]): _s(r.get("line_item_id")) for r in self.promoted_tweets}

    def has_web_event_tags(self) -> bool:
        return any(not t.get("deleted") for t in self.web_event_tags)

    def fetch_tweets(self, promoted_tweet_ids: Iterable[str]):
        """配信があった広告の投稿と、そのカード・メディアの名前・ファイル名（広告名の元。設計書 1.12）"""
        wanted = set(promoted_tweet_ids)
        tweet_ids = sorted({str(p["tweet_id"]) for p in self.promoted_tweets if str(p["id"]) in wanted and p.get("tweet_id")})
        tweets: List[Dict[str, Any]] = []
        for ids in chunks(tweet_ids, MAX_TWEET_IDS):
            tweets += self.client.get_list(f"accounts/{self.account_id}/tweets",
                                           {"tweet_ids": ",".join(ids), "tweet_type": "PUBLISHED",
                                            "timeline_type": "ALL", "trim_user": "true"}, count=0)
        # 同じ投稿が重複して返ることがある
        tweets = list({str(t.get("tweet_id") or t.get("id_str")): t for t in tweets}.values())

        card_uris = sorted({t["card_uri"] for t in tweets if t.get("card_uri")})
        cards: Dict[str, Dict[str, Any]] = {}
        for uris in chunks(card_uris, MAX_CARD_URIS):
            try:
                for c in self.client.get_list(f"accounts/{self.account_id}/cards", {"card_uris": ",".join(uris)}, count=0):
                    cards[c.get("card_uri")] = c
            except XApiError as e:
                logger.warning(f"[{self.account_id}] カードを取得できませんでした: {e}")

        media: Dict[str, Optional[Dict[str, Any]]] = {}
        for t in tweets:
            card = cards.get(t.get("card_uri"))
            keys = _media_keys(card) if card else []
            if not keys:
                keys = _media_keys(t)  # カードのない投稿は投稿のメディアから（設計書 8. の 4）
            for k in keys:
                if k not in media:
                    media[k] = self._media(k)
            t["_card"] = card
            t["_media_keys"] = keys
        self.tweets = tweets
        self._media_by_key = media

    def _media(self, key: str) -> Optional[Dict[str, Any]]:
        try:
            return self.client.get(f"accounts/{self.account_id}/media_library/{key}").get("data")
        except XApiError as e:
            # メディアライブラリにないメディア（管理画面の外で投稿したものなど）はファイル名が取れない
            logger.info(f"[{self.account_id}] メディア {key} を取得できませんでした: {e}")
            return None

    # ---- スナップショットの行 ----
    def rows(self, snapshot_date: str, meta: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        base = {"account_id": self.account_id, "snapshot_date": snapshot_date, **meta}
        out: Dict[str, List[Dict[str, Any]]] = {}
        out["x_ads_campaign_snapshots"] = [{
            **base, "campaign_id": _s(r["id"]), "campaign_name": r.get("name"),
            "entity_status": r.get("entity_status"), "effective_status": r.get("effective_status"), "servable": r.get("servable"),
            "daily_budget_amount_local_micro": _i(r.get("daily_budget_amount_local_micro")),
            "total_budget_amount_local_micro": _i(r.get("total_budget_amount_local_micro")),
            "budget_optimization": r.get("budget_optimization"), "funding_instrument_id": _s(r.get("funding_instrument_id")),
            "currency": r.get("currency"), **_tail(r),
        } for r in self.campaigns]
        out["x_ads_line_item_snapshots"] = [{
            **base, "line_item_id": _s(r["id"]), "line_item_name": r.get("name"), "campaign_id": _s(r.get("campaign_id")),
            "objective": r.get("objective"), "goal": r.get("goal"), "bid_strategy": r.get("bid_strategy"),
            "bid_amount_local_micro": _i(r.get("bid_amount_local_micro")), "target_cpa_local_micro": _i(r.get("target_cpa_local_micro")),
            "pay_by": r.get("pay_by"), "placements": [str(p) for p in r.get("placements") or []],
            "product_type": r.get("product_type"), "primary_web_event_tag": _s(r.get("primary_web_event_tag")),
            "ios_app_store_identifier": _s(r.get("ios_app_store_identifier")),
            "android_app_store_identifier": _s(r.get("android_app_store_identifier")),
            "start_time": r.get("start_time"), "end_time": r.get("end_time"),
            "entity_status": r.get("entity_status"), "currency": r.get("currency"), **_tail(r),
        } for r in self.line_items]
        out["x_ads_promoted_tweet_snapshots"] = [{
            **base, "promoted_tweet_id": _s(r["id"]), "line_item_id": _s(r.get("line_item_id")), "tweet_id": _s(r.get("tweet_id")),
            "approval_status": r.get("approval_status"), "entity_status": r.get("entity_status"), **_tail(r),
        } for r in self.promoted_tweets]
        out["x_ads_tweet_snapshots"] = [self._tweet_row(base, t) for t in self.tweets]
        out["x_ads_web_event_tag_snapshots"] = [{
            **base, "web_event_tag_id": _s(r["id"]), "web_event_tag_name": r.get("name"), "type": r.get("type"),
            "status": r.get("status"), "click_window": _i(r.get("click_window")),
            "view_through_window": _i(r.get("view_through_window")), "retargeting_enabled": r.get("retargeting_enabled"),
            "last_tracked_at": r.get("last_tracked_at"), **_tail(r),
        } for r in self.web_event_tags]
        out["x_ads_app_event_tag_snapshots"] = [{
            **base, "app_event_tag_id": _s(r["id"]), "conversion_type": r.get("conversion_type"), "os_type": r.get("os_type"),
            "app_store_identifier": _s(r.get("app_store_identifier")), "provider_name": r.get("provider_name"),
            "provider_app_event_id": _s(r.get("provider_app_event_id")), "provider_app_event_name": r.get("provider_app_event_name"),
            "post_engagement_attribution_window": _i(r.get("post_engagement_attribution_window")),
            "post_view_attribution_window": _i(r.get("post_view_attribution_window")),
            "retargeting_enabled": r.get("retargeting_enabled"), "status": r.get("status"),
            "last_tracked_at": r.get("last_tracked_at"), **_tail(r),
        } for r in self.app_event_tags]
        return out

    def _tweet_row(self, base: Dict[str, Any], t: Dict[str, Any]) -> Dict[str, Any]:
        card = t.pop("_card", None)
        keys = t.pop("_media_keys", [])
        media = [self._media_by_key.get(k) or {} for k in keys]
        return {
            **base, "tweet_id": _s(t.get("tweet_id") or t.get("id_str")), "tweet_name": t.get("name") or None,
            "full_text": t.get("full_text"), "card_uri": t.get("card_uri"),
            "card_name": (card or {}).get("name"), "card_type": (card or {}).get("card_type"),
            "media_keys": keys, "media_names": [m.get("name") or "" for m in media],
            "media_file_names": [m.get("file_name") or "" for m in media],
            "tweet_created_at": _tweet_time(t.get("created_at")),
            "raw": {"tweet": t, "card": card, "media": media},
        }


def _media_keys(value: Any) -> List[str]:
    """JSON の中の media_key を出てきた順に（重複なしで）集める"""
    found: List[str] = []

    def walk(v: Any):
        if isinstance(v, dict):
            for k, c in v.items():
                if k == "media_key" and isinstance(c, str) and c not in found:
                    found.append(c)
                else:
                    walk(c)
        elif isinstance(v, list):
            for c in v:
                walk(c)
    walk(value)
    return found
