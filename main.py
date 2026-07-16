# -*- coding: utf-8 -*-
"""
TERMINAL // MARKET INTELLIGENCE
--------------------------------
Bloomberg端末風のリアルタイム株価・ニュース監視サーバー。

主な設計方針:
  - すべての設定値は本ファイル冒頭の CONFIG セクションに集約し、環境変数で上書き可能にする
  - 例外は必ず logging に記録する（print は使わない）
  - 誰も購読していないクエリの監視スレッドは一定時間で自動的に停止し、スレッドリークを防ぐ
  - 外部通信（yfinance / Google News RSS）はリトライ・タイムアウトを必ず設定する
  - 外部データ（Excelマスタ, yfinance）が取得できなくてもアプリ全体は落とさず、
    呼び出し元に分かりやすい status を返す
"""
import json
import logging
import re
import time
import threading
import queue
import os
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus
from collections import defaultdict
from typing import Dict, List, Optional, Set

import yfinance as yf
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from flask import Flask, Response, render_template, request, stream_with_context, jsonify
import pandas as pd
import pytz

# ============================================================
# CONFIG
# ============================================================
EXCEL_MASTER_PATH = os.environ.get("STOCK_MASTER_PATH", "data_j.xls")
DEFAULT_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "8"))
STOCK_POLL_INTERVAL_SEC = float(os.environ.get("STOCK_POLL_INTERVAL_SEC", "1"))
NEWS_POLL_INTERVAL_SEC = float(os.environ.get("NEWS_POLL_INTERVAL_SEC", "60"))
IDLE_SLEEP_SEC = 5                 # 購読者がいない間の待機間隔
IDLE_MONITOR_TTL_SEC = int(os.environ.get("IDLE_MONITOR_TTL_SEC", "600"))  # 10分無購読で監視停止
NEWS_LOOKBACK_HOURS = 24           # 初回接続時に遡ってニュースを新着扱いする範囲
NEWS_PAGE_SIZE = 20
SSE_HEARTBEAT_SEC = 15
HISTORY_INTERVAL_DEFAULT = "5m"
HISTORY_PERIOD_DEFAULT = "1d"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("terminal")

app = Flask(__name__)

# ============================================================
# HTTP SESSION（接続プール + リトライ/バックオフ）
# ============================================================
session = requests.Session()
_retry_cfg = Retry(
    total=2,
    connect=2,
    read=2,
    backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET"]),
)
_adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=_retry_cfg)
session.mount("http://", _adapter)
session.mount("https://", _adapter)
session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.8"})


def now_iso() -> str:
    return datetime.now().strftime("%H:%M:%S")


def clean_text(text: str, limit: int = 300) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        text = text[:limit].rstrip() + "…"
    return text


# ============================================================
# 銘柄マスタ（Excel）ロード & 正規化検索
# ============================================================

def normalize(s) -> str:
    """全角/半角・カタカナ/ひらがな・大文字/小文字の揺れを吸収する正規化。"""
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKC", s).lower()
    res = []
    for char in s:
        code = ord(char)
        if 0x30A1 <= code <= 0x30F6:  # カタカナ -> ひらがな
            res.append(chr(code - 0x60))
        else:
            res.append(char)
    return "".join(res)


class StockMaster:
    """東証銘柄マスタ（data_j.xls 等）の遅延ロード + mtime に基づく自動再読込。"""

    def __init__(self, path: str):
        self.path = path
        self._df: Optional[pd.DataFrame] = None
        self._mtime: Optional[float] = None
        self._lock = threading.Lock()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            logger.warning("銘柄マスタが見つかりません: %s （銘柄名検索は無効化されます）", self.path)
            self._df = None
            return
        try:
            df = pd.read_excel(self.path)
            code_col = df.columns[1]
            name_col = df.columns[2]
            df["_norm_name"] = df[name_col].astype(str).apply(normalize)
            df["_norm_code"] = df[code_col].astype(str).apply(normalize)
            self._df = df
            self._mtime = os.path.getmtime(self.path)
            logger.info("銘柄マスタを読み込みました: %s (%d件)", self.path, len(df))
        except Exception:
            logger.exception("銘柄マスタの読み込みに失敗しました: %s", self.path)
            self._df = None

    def _ensure_fresh(self) -> None:
        with self._lock:
            if self._df is None:
                self._load()
                return
            try:
                mtime = os.path.getmtime(self.path)
                if mtime != self._mtime:
                    self._load()
            except OSError:
                pass

    @property
    def df(self) -> Optional[pd.DataFrame]:
        self._ensure_fresh()
        return self._df

    def search(self, query: str, limit: int = 20) -> List[dict]:
        df = self.df
        if df is None or df.empty:
            return []
        code_col = df.columns[1]
        name_col = df.columns[2]
        q_norm = normalize(query)
        matches = df[
            df["_norm_name"].str.contains(q_norm, na=False, regex=False)
            | df["_norm_code"].str.contains(q_norm, na=False, regex=False)
        ].head(limit)
        return [
            {"code": str(row[code_col]), "name": str(row[name_col])}
            for _, row in matches.iterrows()
        ]

    def resolve_code(self, query: str) -> str:
        if len(query) == 4 and query.isalnum() and query.isascii():
            return query
        hits = self.search(query, limit=1)
        if hits:
            return hits[0]["code"]
        return query


stock_master = StockMaster(EXCEL_MASTER_PATH)


def to_ticker_symbol(query: str) -> str:
    """検索クエリ（銘柄名 or コード）を yfinance のティッカーシンボルへ変換する。"""
    resolved = stock_master.resolve_code(query)
    if len(resolved) == 4 and resolved.isalnum() and resolved.isascii():
        return f"{resolved}.T"
    return resolved


# ============================================================
# 市場ステータス
# ============================================================
_JST = pytz.timezone("Asia/Tokyo")


def get_market_status() -> str:
    now = datetime.now(_JST)
    if now.weekday() >= 5:
        return "CLOSED"
    t = now.time()
    if datetime.strptime("09:00", "%H:%M").time() <= t <= datetime.strptime("11:30", "%H:%M").time():
        return "OPEN"
    if datetime.strptime("12:30", "%H:%M").time() <= t <= datetime.strptime("15:00", "%H:%M").time():
        return "OPEN"
    if datetime.strptime("11:30", "%H:%M").time() < t < datetime.strptime("12:30", "%H:%M").time():
        return "LUNCH"
    return "CLOSED"


# ============================================================
# スクレイパー基底クラス
# ============================================================
class BaseScraper:
    site_name: str = "Unknown"
    icon: str = "◆"
    color: str = "#ff9900"

    def result(self, *, items=None, summary="", hero_image=None, pages_fetched=0, status="ok") -> dict:
        items = items or []
        return {
            "site": self.site_name,
            "icon": self.icon,
            "color": self.color,
            "status": status,
            "hero_image": hero_image,
            "summary": summary,
            "pages_fetched": pages_fetched,
            "total_items": len(items),
            "items": items,
            "fetched_at": now_iso(),
        }

    def empty(self, msg="No data found") -> dict:
        return self.result(summary=msg, status="empty")

    def error(self, msg="Connection failed") -> dict:
        return self.result(summary=msg, status="error")


# ============================================================
# ニュースプロバイダ
# ============================================================
class NewsScraper(BaseScraper):
    site_name = "News"
    icon = "⌘"
    color = "#00c2ff"
    PAGE_SIZE = NEWS_PAGE_SIZE

    def fetch(self, query: str) -> dict:
        try:
            rss = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=ja&gl=JP&ceid=JP:ja"
            r = session.get(rss, timeout=DEFAULT_TIMEOUT)
            if r.status_code != 200:
                return self.error(f"HTTP {r.status_code}")
            soup = BeautifulSoup(r.content, "xml")
            entries = soup.find_all("item")[: self.PAGE_SIZE]
            if not entries:
                return self.empty("ニュースが見つかりませんでした")

            items = []
            for it in entries:
                pub_date_str = it.find("pubDate").text if it.find("pubDate") else ""
                pub_dt: Optional[datetime] = None
                if pub_date_str:
                    try:
                        pub_dt = parsedate_to_datetime(pub_date_str)
                    except Exception:
                        pub_dt = None
                items.append(
                    {
                        "title": it.find("title").text if it.find("title") else "",
                        "url": it.find("link").text if it.find("link") else "",
                        "snippet": clean_text(it.find("description").text if it.find("description") else "", 150),
                        "meta": it.find("source").text if it.find("source") else "",
                        "image": None,
                        "pub_date": pub_dt.isoformat() if pub_dt else "",
                        "_pub_dt": pub_dt,  # 内部ソート用（送信時に除去）
                    }
                )
            items.sort(
                key=lambda x: x["_pub_dt"] or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )
            return self.result(items=items, pages_fetched=1, summary=f"{len(items)}件のニュースを取得")
        except requests.RequestException as e:
            logger.warning("ニュース取得に失敗 query=%r: %s", query, e)
            return self.error(str(e))
        except Exception:
            logger.exception("ニュース取得中に予期しないエラー query=%r", query)
            return self.error("internal error")


# ============================================================
# 株価プロバイダ
# ============================================================
class YahooStockProvider(BaseScraper):
    site_name = "Stock"
    icon = "¥"
    color = "#ffcc00"

    def __init__(self):
        self.ticker_cache: Dict[str, yf.Ticker] = {}
        self.info_cache: Dict[str, str] = {}

    def get_ticker(self, symbol: str) -> yf.Ticker:
        if symbol not in self.ticker_cache:
            self.ticker_cache[symbol] = yf.Ticker(symbol)
        return self.ticker_cache[symbol]

    @staticmethod
    def _format_num(val) -> str:
        return f"{val:,.1f}" if isinstance(val, (int, float)) else str(val)

    @staticmethod
    def _format_signed_num(val: float) -> str:
        return f"+{val:,.1f}" if val > 0 else f"{val:,.1f}"

    @staticmethod
    def _format_pct(val: float) -> str:
        return f"+{val:.2f}%" if val > 0 else f"{val:.2f}%"

    def fetch(self, query: str) -> dict:
        try:
            ticker_symbol = to_ticker_symbol(query)
            ticker = self.get_ticker(ticker_symbol)

            hist = ticker.history(period="2d")
            if hist.empty:
                return self.empty(f"株価データが見つかりませんでした: {query}")

            latest = hist.iloc[-1]
            price = float(latest["Close"])

            change = 0.0
            change_pct = 0.0
            if len(hist) > 1:
                prev_close = float(hist.iloc[-2]["Close"])
                change = price - prev_close
                change_pct = (change / prev_close) * 100 if prev_close else 0.0

            if ticker_symbol not in self.info_cache:
                try:
                    self.info_cache[ticker_symbol] = ticker.info.get("shortName", query)
                except Exception:
                    self.info_cache[ticker_symbol] = query

            name = self.info_cache[ticker_symbol]

            return {
                "status": "ok",
                "name": name,
                "code": ticker_symbol,
                "price": self._format_num(price),
                "change": self._format_signed_num(change),
                "change_pct": self._format_pct(change_pct),
                "raw_change": change,
                "raw_price": price,
            }
        except Exception as e:
            logger.warning("株価取得に失敗 query=%r: %s", query, e)
            return self.error(str(e))


# 単発クエリ（/api/quote, /api/history）用に共有インスタンスを使い回し、
# ティッカー/銘柄名キャッシュを無駄に再生成しない
shared_stock_provider = YahooStockProvider()

# ============================================================
# イベントブローカー（SSE配信）
# ============================================================
_last_news_cache: Dict[str, dict] = {}


class EventManager:
    def __init__(self):
        self.clients: Dict[str, List[queue.Queue]] = defaultdict(list)
        self.lock = threading.Lock()

    def subscribe(self, query: str, q: queue.Queue) -> None:
        with self.lock:
            self.clients[query].append(q)

    def unsubscribe(self, query: str, q: queue.Queue) -> None:
        with self.lock:
            if query in self.clients and q in self.clients[query]:
                self.clients[query].remove(q)
                if not self.clients[query]:
                    del self.clients[query]

    def has_subscribers(self, query: str) -> bool:
        with self.lock:
            return len(self.clients.get(query, [])) > 0

    def broadcast(self, query: str, event_type: str, data: dict) -> None:
        msg = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
        with self.lock:
            for q in self.clients.get(query, []):
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    logger.warning("SSEキューが満杯のためメッセージを破棄 query=%r", query)


event_manager = EventManager()


# ============================================================
# 監視スレッド管理（アイドルクエリの自動停止でスレッドリークを防止）
# ============================================================
class MonitorRegistry:
    """
    各クエリにつき「株価監視」「ニュース監視」の2スレッドを起動する。
    誰も購読していない状態が IDLE_MONITOR_TTL_SEC を超えたスレッドは自ら終了し、
    その旨をレジストリへ報告する。両方が終了して初めてクエリを「未起動」に戻す。
    """

    def __init__(self):
        self._alive_threads: Dict[str, int] = defaultdict(int)
        self.lock = threading.Lock()

    def ensure_monitors(self, query: str) -> None:
        with self.lock:
            if self._alive_threads.get(query, 0) > 0:
                return
            self._alive_threads[query] = 2
            threading.Thread(
                target=self._run_stock_monitor, args=(query,), daemon=True, name=f"stock-monitor:{query}"
            ).start()
            threading.Thread(
                target=self._run_news_monitor, args=(query,), daemon=True, name=f"news-monitor:{query}"
            ).start()
            logger.info("監視スレッドを起動しました query=%r", query)

    def _mark_stopped(self, query: str) -> None:
        with self.lock:
            if query in self._alive_threads:
                self._alive_threads[query] -= 1
                if self._alive_threads[query] <= 0:
                    del self._alive_threads[query]
                    logger.info("query=%r の監視スレッドが全て停止しました（アイドル）", query)

    def _run_stock_monitor(self, query: str) -> None:
        try:
            stock_monitor_loop(query)
        finally:
            self._mark_stopped(query)

    def _run_news_monitor(self, query: str) -> None:
        try:
            news_monitor_loop(query)
        finally:
            self._mark_stopped(query)


registry = MonitorRegistry()


def _wait_while_idle(query: str) -> bool:
    """
    購読者が現れるまで待機する。IDLE_MONITOR_TTL_SEC 秒を超えて誰も現れなければ
    False を返し、呼び出し元のループを終了させる。
    """
    idle_since = time.time()
    while not event_manager.has_subscribers(query):
        if time.time() - idle_since > IDLE_MONITOR_TTL_SEC:
            return False
        time.sleep(IDLE_SLEEP_SEC)
    return True


def stock_monitor_loop(query: str) -> None:
    provider = YahooStockProvider()
    last_status = None

    while True:
        if not _wait_while_idle(query):
            logger.info("株価監視をアイドルタイムアウトで終了 query=%r", query)
            return

        status = get_market_status()
        if status != last_status:
            event_manager.broadcast(query, "market_status", {"status": status})
            last_status = status

        try:
            data = provider.fetch(query)
            if data.get("status") == "ok":
                event_manager.broadcast(query, "stock", data)
        except Exception:
            logger.exception("株価監視ループでエラー query=%r", query)

        time.sleep(STOCK_POLL_INTERVAL_SEC)


def news_monitor_loop(query: str) -> None:
    scraper = NewsScraper()
    seen_urls: Set[str] = set()
    last_fetch_time: Optional[datetime] = None

    while True:
        if not _wait_while_idle(query):
            logger.info("ニュース監視をアイドルタイムアウトで終了 query=%r", query)
            return

        try:
            now_utc = datetime.now(timezone.utc)
            data = scraper.fetch(query)
            if data.get("status") == "ok":
                cutoff = last_fetch_time or (now_utc - timedelta(hours=NEWS_LOOKBACK_HOURS))

                new_items = []
                for item in data.get("items", []):
                    url = item.get("url", "")
                    pub_dt: Optional[datetime] = item.pop("_pub_dt", None)

                    if not url or url in seen_urls:
                        seen_urls.add(url)
                        continue
                    if pub_dt is not None and pub_dt <= cutoff:
                        seen_urls.add(url)
                        continue

                    seen_urls.add(url)
                    new_items.append(item)

                last_fetch_time = now_utc

                if new_items:
                    send_data = dict(data)
                    send_data["items"] = new_items
                    send_data["total_items"] = len(new_items)
                    send_data["summary"] = f"{len(new_items)}件の新着ニュース"
                    _last_news_cache[query] = send_data
                    event_manager.broadcast(query, "result", send_data)
                    event_manager.broadcast(query, "log", {"message": f"{len(new_items)}件の新着記事を取得しました"})
        except Exception:
            logger.exception("ニュース監視ループでエラー query=%r", query)

        time.sleep(NEWS_POLL_INTERVAL_SEC)


# ============================================================
# Flask ルーティング
# ============================================================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/suggest")
def api_suggest():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    return jsonify(stock_master.search(query, limit=20))


@app.route("/api/quote")
def api_quote():
    """ウォッチリスト追加時などに使う単発の株価取得（SSEなし）。"""
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"status": "error", "message": "query is empty"}), 400
    data = shared_stock_provider.fetch(query)
    return jsonify(data)


@app.route("/api/history")
def api_history():
    """ウォッチリストのスパークライン初期描画用に、当日の値動きを返す。"""
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"status": "error", "message": "query is empty"}), 400

    try:
        ticker_symbol = to_ticker_symbol(query)
        ticker = shared_stock_provider.get_ticker(ticker_symbol)

        hist = ticker.history(period=HISTORY_PERIOD_DEFAULT, interval=HISTORY_INTERVAL_DEFAULT)
        if hist.empty:
            # 休場日などで当日分が無ければ直近5日の日足にフォールバック
            hist = ticker.history(period="5d", interval="1d")

        points = [
            {"t": idx.strftime("%m/%d %H:%M"), "p": round(float(row["Close"]), 2)}
            for idx, row in hist.iterrows()
            if pd.notna(row["Close"])
        ]
        return jsonify({"status": "ok", "code": ticker_symbol, "points": points})
    except Exception as e:
        logger.warning("履歴取得に失敗 query=%r: %s", query, e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/search-stream")
def search_stream():
    query = request.args.get("q", "").strip()

    def generate():
        if not query:
            yield f"event: error\ndata: {json.dumps({'message': 'query is empty'})}\n\n"
            return

        yield f"event: start\ndata: {json.dumps({'query': query})}\n\n"
        yield ":" + " " * 2048 + "\n\n"  # プロキシのバッファリング回避

        q: queue.Queue = queue.Queue(maxsize=100)
        event_manager.subscribe(query, q)
        registry.ensure_monitors(query)

        yield f"event: log\ndata: {json.dumps({'message': f'Subscribed to real-time events for {query}'})}\n\n"

        if query in _last_news_cache:
            cached = _last_news_cache[query]
            yield f"event: result\ndata: {json.dumps(cached, ensure_ascii=False)}\n\n"

        try:
            while True:
                try:
                    msg = q.get(timeout=SSE_HEARTBEAT_SEC)
                    yield msg
                except queue.Empty:
                    yield f"event: heartbeat\ndata: {json.dumps({'timestamp': now_iso()})}\n\n"
        except GeneratorExit:
            pass
        finally:
            event_manager.unsubscribe(query, q)
            logger.info("クライアントが切断しました query=%r", query)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5050"))
    debug = os.environ.get("FLASK_DEBUG", "1") == "1"
    logger.info("起動: port=%d debug=%s", port, debug)
    app.run(debug=debug, port=port, threaded=True)