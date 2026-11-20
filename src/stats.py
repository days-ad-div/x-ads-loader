"""非同期レポートのリクエストと、結果の展開（設計書 3.3）"""
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple
from zoneinfo import ZoneInfo

# Source の階層（API の名前）→ API の entity
LEVELS = {"campaign": "CAMPAIGN", "line_item": "LINE_ITEM", "promoted_tweet": "PROMOTED_TWEET"}
# 通常のレポートで取る指標グループ（6 つ同時の指定で 3 階層とも HTTP 200 を確認済み）
METRIC_GROUPS = "ENGAGEMENT,BILLING,VIDEO,MEDIA,WEB_CONVERSION,MOBILE_CONVERSION"
# 同時に指定できるエンティティ ID の数
MAX_ENTITY_IDS = 20


def zone(name: Optional[str]) -> ZoneInfo:
    try:
        return ZoneInfo(name or "Asia/Tokyo")
    except Exception:
        return ZoneInfo("Asia/Tokyo")


def time_window(tz: ZoneInfo, start: date, end: date) -> Dict[str, str]:
    """start〜end（両端を含む）を、アカウントのタイムゾーンの 0 時で区切った UTC の時刻で返す（DAY 集計は 0 時区切りが必須）"""
    def utc(d: date) -> str:
        return datetime.combine(d, dtime.min, tz).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"start_time": utc(start), "end_time": utc(end + timedelta(days=1))}


def split_period(start: date, end: date, max_days: int) -> List[Tuple[date, date]]:
    periods = []
    cur = start
    while cur <= end:
        last = min(end, cur + timedelta(days=max_days - 1))
        periods.append((cur, last))
        cur = last + timedelta(days=1)
    return periods


def chunks(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _pick(value: Any, i: int) -> Any:
    """日別の配列から i 日目の値を取り出す。オブジェクト（CV）は中の配列ごとに取り出して同じ形で返す"""
    if isinstance(value, list):
        return value[i] if i < len(value) else None
    if isinstance(value, dict):
        return {k: _pick(v, i) for k, v in value.items()}
    return None


def _compact(value: Any) -> Any:
    """null の項目と、中身が全部 null のオブジェクトを省く"""
    if isinstance(value, dict):
        out = {k: c for k, c in ((k, _compact(v)) for k, v in value.items()) if c is not None}
        return out or None
    return value


def _has_value(value: Any) -> bool:
    if isinstance(value, dict):
        return any(_has_value(v) for v in value.values())
    if isinstance(value, (int, float)):
        return value != 0
    return value is not None and value != "0"


def expand(result: Dict[str, Any], start: date) -> Iterator[Tuple[str, Optional[Dict[str, Any]], date, Dict[str, Any]]]:
    """非同期レポートの結果を (エンティティ ID, セグメント, 日付, その日の指標) に展開する。

    指標は期間初日からの日別配列で返る。その日の指標がすべて null か 0 なら行を作らない（配信のない日）
    """
    length = result.get("time_series_length")
    for entity in result.get("data") or []:
        for id_data in entity.get("id_data") or []:
            metrics = id_data.get("metrics") or {}
            n = length or max((len(v) for v in _arrays(metrics)), default=0)
            for i in range(n):
                day_metrics = _compact({k: _pick(v, i) for k, v in metrics.items()}) or {}
                if _has_value(day_metrics):
                    yield str(entity["id"]), id_data.get("segment"), start + timedelta(days=i), day_metrics


def _arrays(value: Any) -> Iterator[list]:
    if isinstance(value, list):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from _arrays(v)
