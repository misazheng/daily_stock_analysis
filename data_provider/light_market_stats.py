# -*- coding: utf-8 -*-
"""轻量全市场统计兜底：涨跌家数 / 涨跌停家数 / 成交额合计。

背景
----
`DataFetcherManager.get_market_stats()` 原有数据源（efinance、akshare）都依赖
「全市场实时快照」重接口：单次请求要拉 5900+ 只股票 × 20+ 字段，响应 1.5MB 以上；
akshare 侧还有新浪全量分页兜底。在 GitHub Actions 等**海外网络出口**下，这类重请求
经常在 30s 超时上限内拉不完、或被限流，最终 `get_market_stats()` 返回空，
大盘复盘里的涨跌家数、涨跌停结构、成交额就全部记 0（表现为「数据缺失」）。

本模块改用「最小字段 + 分页」策略，并用**两个互不相关的域名**依次尝试：

1. 东财 ``push2.eastmoney.com/api/qt/clist/get``（每页 1000 只、仅 5 个字段，约 6 个请求）
2. 新浪 ``Market_Center.getHQNodeData``（每股 5 个字段，每页 100 只，约 56 个请求）

两源都失败时返回 None，上层自动回退到原有数据源，行为与打补丁前完全一致。

安全边界
--------
- 任一源**只要有一页没拿到**就丢弃该源的全部数据（不拼接半份数据）；
- 家数之和低于 ``MARKET_STATS_LIGHT_MIN_ROWS``（默认 4000）判定为不完整，返回 None；
- 返回 None 时不会影响原有逻辑，也不会把 0 当成真实数据写进报告。

统计口径
--------
- 优先委托 ``AkshareFetcher._calc_market_stats()`` 计算，与项目原有口径**逐字一致**；
- 委托不可用时退回内置实现（同样的 A 股涨跌停规则：北交所 ±30%、科创/创业板 ±20%、
  ST ±5%、其余 ±10%，按昨收四舍五入到分后精确比对；停牌股按成交额为 0 跳过）。

环境变量
--------
- ``MARKET_STATS_LIGHT=0``             关闭本兜底（恢复打补丁前的行为）
- ``MARKET_STATS_LIGHT_SOURCES``       数据源顺序，默认 ``eastmoney,sina``
- ``MARKET_STATS_LIGHT_PAGE_SIZE``     东财每页条数，默认 1000（新浪固定 100）
- ``MARKET_STATS_LIGHT_TIMEOUT``       单次请求超时秒数，默认 10
- ``MARKET_STATS_LIGHT_RETRIES``       每页失败重试次数，默认 3
- ``MARKET_STATS_LIGHT_MIN_ROWS``      完整性下限（家数之和），默认 4000
- ``MARKET_STATS_LIGHT_PAGE_DELAY``    分页之间停顿秒数，默认 0.15
- ``MARKET_STATS_LIGHT_TRUST_ENV=0``   忽略环境变量里的 HTTP(S)_PROXY
"""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_LOG_TAG = "[MarketStats] component=market_stats provider=LightAggregate"

# 沪深京全部 A 股（与 akshare / efinance 使用的口径一致）
_ALL_A_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
# 只取 5 个字段，响应体积约为完整快照的 1/5
_EM_FIELDS = "f2,f6,f12,f14,f18"
_EM_HOSTS = (
    "push2.eastmoney.com",
    "92.push2.eastmoney.com",
    "82.push2.eastmoney.com",
    "push2delay.eastmoney.com",
)
_SINA_URL = (
    "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    "Market_Center.getHQNodeData?page={page}&num={num}&sort=symbol&asc=1&node=hs_a"
)

_USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
)

_DEFAULT_PAGE_SIZE = 1000          # 东财每页条数
_SINA_PAGE_SIZE = 100              # 新浪服务端硬上限（传更大也只返回 100）
_DEFAULT_TIMEOUT = 10.0
_DEFAULT_RETRIES = 3
_DEFAULT_MIN_ROWS = 4000
_DEFAULT_PAGE_DELAY = 0.15
_MAX_PAGES = 80


# --------------------------------------------------------------------------- #
# 环境变量
# --------------------------------------------------------------------------- #
def _env_str(name: str, default: str) -> str:
    value = str(os.getenv(name, "")).strip()
    return value or default


def _env_int(name: str, default: int) -> int:
    try:
        value = int(str(os.getenv(name, "")).strip())
        return value if value > 0 else default
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        value = float(str(os.getenv(name, "")).strip())
        return value if value > 0 else default
    except Exception:
        return default


def is_enabled() -> bool:
    """是否启用轻量兜底（默认启用；置 0/false/off/no 可关闭）。"""
    return str(os.getenv("MARKET_STATS_LIGHT", "1")).strip().lower() not in (
        "0", "false", "off", "no",
    )


# --------------------------------------------------------------------------- #
# HTTP 会话 / 工具
# --------------------------------------------------------------------------- #
_SESSION: Optional[requests.Session] = None


def _session() -> requests.Session:
    """复用连接；默认沿用系统代理设置，可用 MARKET_STATS_LIGHT_TRUST_ENV=0 忽略。"""
    global _SESSION
    if _SESSION is None:
        session = requests.Session()
        raw = str(os.getenv("MARKET_STATS_LIGHT_TRUST_ENV", "1")).strip().lower()
        session.trust_env = raw not in ("0", "false", "off", "no")
        _SESSION = session
    return _SESSION


def _headers(referer: str) -> Dict[str, str]:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Referer": referer,
        "Accept": "*/*",
    }


def _to_float(value: Any) -> Optional[float]:
    """停牌股会返回 '-' 或空串，统一转成 None（后续按缺失跳过）。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None
    text = str(value).strip()
    if text in ("", "-", "--", "None", "null", "nan"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _get_json(url: str, referer: str, timeout: float) -> Any:
    resp = _session().get(url, headers=_headers(referer), timeout=timeout)
    resp.raise_for_status()
    return resp.json()


# --------------------------------------------------------------------------- #
# 源 1：东方财富 clist（少字段 + 大分页）
# --------------------------------------------------------------------------- #
def _eastmoney_page(page: int, page_size: int, timeout: float, retries: int) -> Optional[Dict[str, Any]]:
    last_error = "unknown"
    for attempt in range(max(1, retries)):
        host = _EM_HOSTS[attempt % len(_EM_HOSTS)]
        url = (
            f"https://{host}/api/qt/clist/get?pn={page}&pz={page_size}&po=1&np=1"
            f"&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3"
            f"&fs={_ALL_A_FS}&fields={_EM_FIELDS}&_={int(time.time() * 1000)}"
        )
        try:
            payload = _get_json(url, "https://quote.eastmoney.com/", timeout) or {}
            data = payload.get("data") or {}
            diff = data.get("diff") or []
            if diff:
                total = data.get("total")
                try:
                    total = int(total) if total is not None else None
                except Exception:
                    total = None
                return {"rows": diff, "total": total}
            last_error = "empty diff (rc=%s)" % payload.get("rc")
        except Exception as exc:  # noqa: BLE001 - 一切网络异常都重试
            last_error = "%s: %s" % (type(exc).__name__, exc)
        time.sleep(0.4 * (attempt + 1))
    logger.warning("%s source=eastmoney action=page_failed page=%s error=%s", _LOG_TAG, page, last_error)
    return None


def _fetch_eastmoney(page_size: int, timeout: float, retries: int, delay: float) -> Optional[List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    total: Optional[int] = None
    page = 1
    while page <= _MAX_PAGES:
        chunk = _eastmoney_page(page, page_size, timeout, retries)
        if chunk is None:
            logger.warning(
                "%s source=eastmoney action=abort page=%s rows_so_far=%s status=partial_discarded",
                _LOG_TAG, page, len(rows),
            )
            return None
        rows.extend(chunk["rows"])
        if total is None:
            total = chunk["total"]
        if total is not None and len(rows) >= total:
            break
        if len(chunk["rows"]) < page_size:
            break
        page += 1
        time.sleep(delay)
    if not rows:
        return None
    logger.info("%s source=eastmoney action=pages_done rows=%s total=%s", _LOG_TAG, len(rows), total)
    return [
        {
            "代码": row.get("f12"),
            "名称": row.get("f14"),
            "最新价": _to_float(row.get("f2")),
            "昨收": _to_float(row.get("f18")),
            "成交额": _to_float(row.get("f6")),
        }
        for row in rows
    ]


# --------------------------------------------------------------------------- #
# 源 2：新浪全市场分页（独立域名，抗封性不同）
# --------------------------------------------------------------------------- #
def _sina_page(page: int, timeout: float, retries: int) -> Optional[List[Dict[str, Any]]]:
    url = _SINA_URL.format(page=page, num=_SINA_PAGE_SIZE)
    last_error = "unknown"
    for attempt in range(max(1, retries)):
        try:
            payload = _get_json(url, "https://finance.sina.com.cn/", timeout)
            if isinstance(payload, list) and payload:
                return payload
            last_error = "empty payload"
        except Exception as exc:  # noqa: BLE001
            last_error = "%s: %s" % (type(exc).__name__, exc)
        time.sleep(0.4 * (attempt + 1))
    logger.warning("%s source=sina action=page_failed page=%s error=%s", _LOG_TAG, page, last_error)
    return None


def _fetch_sina(timeout: float, retries: int, delay: float) -> Optional[List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    page = 1
    while page <= _MAX_PAGES:
        chunk = _sina_page(page, timeout, retries)
        if chunk is None:
            logger.warning(
                "%s source=sina action=abort page=%s rows_so_far=%s status=partial_discarded",
                _LOG_TAG, page, len(rows),
            )
            return None
        rows.extend(chunk)
        if len(chunk) < _SINA_PAGE_SIZE:
            break
        page += 1
        time.sleep(delay)
    if not rows:
        return None
    logger.info("%s source=sina action=pages_done rows=%s", _LOG_TAG, len(rows))
    return [
        {
            "代码": row.get("code"),
            "名称": row.get("name"),
            "最新价": _to_float(row.get("trade")),
            "昨收": _to_float(row.get("settlement")),
            "成交额": _to_float(row.get("amount")),
        }
        for row in rows
    ]


# --------------------------------------------------------------------------- #
# 统计计算（优先复用项目原有逻辑）
# --------------------------------------------------------------------------- #
def _calc_stats(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    try:
        from .akshare_fetcher import AkshareFetcher

        stats = AkshareFetcher._calc_market_stats(None, df)  # 该方法不依赖实例状态
        if stats:
            return stats
    except Exception as exc:  # noqa: BLE001
        logger.warning("%s action=delegate_failed error=%s fallback=builtin", _LOG_TAG, exc)
    return _calc_stats_builtin(df)


def _calc_stats_builtin(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """内置实现：与 AkshareFetcher._calc_market_stats 同口径。"""
    try:
        import numpy as np

        from .base import is_bse_code, is_kc_cy_stock, is_st_stock, normalize_stock_code
    except Exception as exc:  # noqa: BLE001
        logger.error("%s action=import_failed error=%s", _LOG_TAG, exc)
        return None

    up_count = down_count = flat_count = 0
    limit_up_count = limit_down_count = 0

    for code, name, current_price, pre_close, amount in zip(
        df["代码"], df["名称"], df["最新价"], df["昨收"], df["成交额"]
    ):
        # 停牌/无成交不参与统计，与原实现一致
        if pd.isna(current_price) or pd.isna(pre_close) or pd.isna(amount) or amount == 0:
            continue

        current_price = float(current_price)
        pre_close = float(pre_close)
        pure_code = normalize_stock_code(str(code))

        if is_bse_code(pure_code):
            ratio = 0.30
        elif is_kc_cy_stock(pure_code):
            ratio = 0.20
        elif is_st_stock(name):
            ratio = 0.05
        else:
            ratio = 0.10

        limit_up_price = np.floor(pre_close * (1 + ratio) * 100 + 0.5) / 100.0
        limit_down_price = np.floor(pre_close * (1 - ratio) * 100 + 0.5) / 100.0
        limit_up_tolerance = round(abs(pre_close * (1 + ratio) - limit_up_price), 10)
        limit_down_tolerance = round(abs(pre_close * (1 - ratio) - limit_down_price), 10)

        if current_price > 0:
            if abs(current_price - limit_up_price) <= limit_up_tolerance:
                limit_up_count += 1
            if abs(current_price - limit_down_price) <= limit_down_tolerance:
                limit_down_count += 1
            if current_price > pre_close:
                up_count += 1
            elif current_price < pre_close:
                down_count += 1
            else:
                flat_count += 1

    total_amount = float(pd.to_numeric(df["成交额"], errors="coerce").fillna(0).sum() / 1e8)

    return {
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": flat_count,
        "limit_up_count": limit_up_count,
        "limit_down_count": limit_down_count,
        "total_amount": total_amount,
    }


# --------------------------------------------------------------------------- #
# 对外入口
# --------------------------------------------------------------------------- #
def fetch_light_market_stats(
    *,
    page_size: Optional[int] = None,
    timeout: Optional[float] = None,
    retries: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """轻量拉取全市场统计。

    Returns:
        成功：``{'up_count','down_count','flat_count','limit_up_count',
        'limit_down_count','total_amount'}``（total_amount 单位：亿元）
        失败或数据不完整：None（由上层回退到原有数据源）
    """
    page_size = page_size or _env_int("MARKET_STATS_LIGHT_PAGE_SIZE", _DEFAULT_PAGE_SIZE)
    timeout = timeout or _env_float("MARKET_STATS_LIGHT_TIMEOUT", _DEFAULT_TIMEOUT)
    retries = retries or _env_int("MARKET_STATS_LIGHT_RETRIES", _DEFAULT_RETRIES)
    min_rows = _env_int("MARKET_STATS_LIGHT_MIN_ROWS", _DEFAULT_MIN_ROWS)
    delay = _env_float("MARKET_STATS_LIGHT_PAGE_DELAY", _DEFAULT_PAGE_DELAY)

    sources = [
        name.strip().lower()
        for name in _env_str("MARKET_STATS_LIGHT_SOURCES", "eastmoney,sina").split(",")
        if name.strip()
    ]
    fetchers: Dict[str, Callable[[], Optional[List[Dict[str, Any]]]]] = {
        "eastmoney": lambda: _fetch_eastmoney(page_size, timeout, retries, delay),
        "sina": lambda: _fetch_sina(timeout, retries, delay),
    }

    started_at = time.monotonic()
    for name in sources:
        fetch = fetchers.get(name)
        if fetch is None:
            logger.warning("%s action=unknown_source source=%s", _LOG_TAG, name)
            continue

        source_started = time.monotonic()
        try:
            records = fetch()
        except Exception as exc:  # noqa: BLE001
            logger.warning("%s source=%s action=failed error=%s", _LOG_TAG, name, exc)
            continue
        if not records:
            continue

        df = pd.DataFrame(records, columns=["代码", "名称", "最新价", "昨收", "成交额"])
        stats = _calc_stats(df)
        if not stats:
            continue

        counted = (
            int(stats.get("up_count", 0))
            + int(stats.get("down_count", 0))
            + int(stats.get("flat_count", 0))
        )
        if counted < min_rows:
            logger.warning(
                "%s source=%s action=reject counted=%s min_rows=%s status=incomplete",
                _LOG_TAG, name, counted, min_rows,
            )
            continue

        logger.info(
            "%s source=%s action=success rows=%s counted=%s up=%s down=%s flat=%s "
            "limit_up=%s limit_down=%s amount=%.2f亿元 elapsed=%.2fs",
            _LOG_TAG, name, len(records), counted,
            stats.get("up_count"), stats.get("down_count"), stats.get("flat_count"),
            stats.get("limit_up_count"), stats.get("limit_down_count"),
            float(stats.get("total_amount") or 0.0),
            time.monotonic() - source_started,
        )
        return stats

    logger.warning(
        "%s action=abort status=all_sources_failed sources=%s elapsed=%.2fs",
        _LOG_TAG, ",".join(sources), time.monotonic() - started_at,
    )
    return None
