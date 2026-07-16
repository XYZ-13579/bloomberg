# -*- coding: utf-8 -*-
import json
import re
import time
import html
import traceback
import threading
import queue
import os
import unicodedata
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus
from collections import defaultdict
from typing import Dict, Optional, Set

import yfinance as yf
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from flask import Flask, Response, render_template, request, stream_with_context, jsonify
import pandas as pd

import pytz

app = Flask(__name__)

DEFAULT_TIMEOUT = 8
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Global Session with connection pooling
session = requests.Session()
adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
session.mount('http://', adapter)
session.mount('https://', adapter)
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

# --- Excel Data Loading & Normalization ---

def normalize(s):
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKC", s).lower()
    res = []
    for char in s:
        code = ord(char)
        if 0x30A1 <= code <= 0x30F6:
            res.append(chr(code - 0x60))
        else:
            res.append(char)
    return "".join(res)

_df_stocks = None
def load_excel_data():
    global _df_stocks
    if _df_stocks is None:
        try:
            if os.path.exists("data_j.xls"):
                _df_stocks = pd.read_excel("data_j.xls")
                name_col = _df_stocks.columns[2]
                code_col = _df_stocks.columns[1]
                _df_stocks['_norm_name'] = _df_stocks[name_col].astype(str).apply(normalize)
                _df_stocks['_norm_code'] = _df_stocks[code_col].astype(str).apply(normalize)
        except Exception as e:
            print(f"Error loading data_j.xls: {e}")

# Preload on start
load_excel_data()

def resolve_stock_code(query: str) -> str:
    global _df_stocks
    if len(query) == 4 and query.isalnum() and query.isascii():
        return query
    if _df_stocks is not None and not _df_stocks.empty:
        code_col = _df_stocks.columns[1]
        q_norm = normalize(query)
        matches = _df_stocks[
            _df_stocks['_norm_name'].str.contains(q_norm, na=False, regex=False) |
            _df_stocks['_norm_code'].str.contains(q_norm, na=False, regex=False)
        ]
        if not matches.empty:
            return str(matches.iloc[0][code_col])
    return query

# --- Market Status Detection ---
def get_market_status():
    jst = pytz.timezone('Asia/Tokyo')
    now = datetime.now(jst)
    
    # Check weekends
    if now.weekday() >= 5:
        return "CLOSED" # Holiday / Weekend
        
    t = now.time()
    if t >= datetime.strptime("09:00", "%H:%M").time() and t <= datetime.strptime("11:30", "%H:%M").time():
        return "OPEN"
    elif t >= datetime.strptime("12:30", "%H:%M").time() and t <= datetime.strptime("15:00", "%H:%M").time():
        return "OPEN"
    elif t > datetime.strptime("11:30", "%H:%M").time() and t < datetime.strptime("12:30", "%H:%M").time():
        return "LUNCH"
    else:
        return "CLOSED"

def get_poll_interval():
    return 1 # 1 second

# --- Base Scrapers ---

class BaseScraper:
    site_name: str = "Unknown"
    icon: str = "◆"
    color: str = "#00f0ff"

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

# --- News Provider ---

class NewsScraper(BaseScraper):
    site_name = "News"
    icon = "⌘"
    color = "#22d3ee"
    PAGE_SIZE = 20

    def fetch(self, query: str) -> dict:
        try:
            rss = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=ja&gl=JP&ceid=JP:ja"
            r = session.get(rss, timeout=DEFAULT_TIMEOUT)
            if r.status_code != 200:
                return self.error(f"HTTP {r.status_code}")
            soup = BeautifulSoup(r.content, "xml")
            entries = soup.find_all("item")[:self.PAGE_SIZE]
            if not entries:
                return self.empty("ニュースが見つかりませんでした")
            items = []
            for it in entries:
                # pubDate を解析してタイムゾーン付き datetime に変換
                pub_date_str = it.find("pubDate").text if it.find("pubDate") else ""
                pub_dt: Optional[datetime] = None
                if pub_date_str:
                    try:
                        pub_dt = parsedate_to_datetime(pub_date_str)
                    except Exception:
                        pub_dt = None
                items.append({
                    "title": it.find("title").text if it.find("title") else "",
                    "url": it.find("link").text if it.find("link") else "",
                    "snippet": clean_text(it.find("description").text if it.find("description") else "", 150),
                    "meta": it.find("source").text if it.find("source") else "",
                    "image": None,
                    "pub_date": pub_dt.isoformat() if pub_dt else "",
                    "_pub_dt": pub_dt,  # 内部ソート用（送信時に除去）
                })
            # 公開日時の新しい順に並べ替え（pubDate なし記事は末尾）
            items.sort(
                key=lambda x: x["_pub_dt"] or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )
            return self.result(items=items, pages_fetched=1, summary=f"{len(items)}件のニュースを取得")
        except Exception as e:
            return self.error(str(e))

# --- Stock Providers ---

class BaseStockProvider(BaseScraper):
    site_name = "Stock"
    icon = "¥"
    color = "#ffcc00"
    
    def fetch(self, query: str) -> dict:
        raise NotImplementedError

class YahooStockProvider(BaseStockProvider):
    def __init__(self):
        self.ticker_cache = {}
        self.info_cache = {}

    def get_ticker(self, symbol: str):
        if symbol not in self.ticker_cache:
            self.ticker_cache[symbol] = yf.Ticker(symbol)
        return self.ticker_cache[symbol]

    def fetch(self, query: str) -> dict:
        try:
            resolved_code = resolve_stock_code(query)
            if len(resolved_code) == 4 and resolved_code.isalnum() and resolved_code.isascii():
                ticker_symbol = f"{resolved_code}.T"
            else:
                ticker_symbol = resolved_code
                
            ticker = self.get_ticker(ticker_symbol)
            
            # market プロジェクトと同じ方式: history(period="2d")で前日比を計算
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
                change_pct = (change / prev_close) * 100
            
            if ticker_symbol not in self.info_cache:
                try:
                    self.info_cache[ticker_symbol] = ticker.info.get("shortName", query)
                except Exception:
                    self.info_cache[ticker_symbol] = query
                    
            name = self.info_cache[ticker_symbol]

            def format_num(val):
                return f"{val:,.1f}" if isinstance(val, (int, float)) else str(val)
            def format_signed_num(val):
                if val > 0: return f"+{val:,.1f}"
                return f"{val:,.1f}"
            def format_pct(val):
                if val > 0: return f"+{val:.2f}%"
                return f"{val:.2f}%"

            return {
                "status": "ok",
                "name": name,
                "code": ticker_symbol,
                "price": format_num(price),
                "change": format_signed_num(change),
                "change_pct": format_pct(change_pct),
                "raw_change": change
            }
        except Exception as e:
            return self.error(str(e))

# --- News Cache (last result per query for new subscribers) ---
_last_news_cache: Dict[str, dict] = {}

# --- Event Broker ---

class EventManager:
    def __init__(self):
        self.clients = defaultdict(list)
        self.lock = threading.Lock()

    def subscribe(self, query: str, q: queue.Queue):
        with self.lock:
            self.clients[query].append(q)

    def unsubscribe(self, query: str, q: queue.Queue):
        with self.lock:
            if query in self.clients and q in self.clients[query]:
                self.clients[query].remove(q)
                if not self.clients[query]:
                    del self.clients[query]

    def has_subscribers(self, query: str):
        with self.lock:
            return len(self.clients.get(query, [])) > 0

    def broadcast(self, query: str, event_type: str, data: dict):
        msg = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
        with self.lock:
            for q in self.clients.get(query, []):
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    pass

event_manager = EventManager()

# --- Monitor Threads ---

class MonitorRegistry:
    def __init__(self):
        self.active_queries = set()
        self.lock = threading.Lock()

    def ensure_monitors(self, query: str):
        with self.lock:
            if query not in self.active_queries:
                self.active_queries.add(query)
                threading.Thread(target=stock_monitor_loop, args=(query,), daemon=True).start()
                threading.Thread(target=news_monitor_loop, args=(query,), daemon=True).start()

registry = MonitorRegistry()

def stock_monitor_loop(query: str):
    provider = YahooStockProvider()
    last_status = None
    
    while True:
        if not event_manager.has_subscribers(query):
            time.sleep(5)
            continue
            
        status = get_market_status()
        if status != last_status:
            event_manager.broadcast(query, "market_status", {"status": status})
            last_status = status
            
        try:
            data = provider.fetch(query)
            if data.get("status") == "ok":
                # 毎秒必ず送信（market プロジェクトと同じ方式）
                event_manager.broadcast(query, "stock", data)
        except Exception:
            pass
            
        time.sleep(get_poll_interval())

def news_monitor_loop(query: str):
    scraper = NewsScraper()
    seen_urls: Set[str] = set()          # 送信済みURL（永続蓄積→重複完全排除）
    last_fetch_time: Optional[datetime] = None  # 前回フェッチ完了時刻

    while True:
        if not event_manager.has_subscribers(query):
            time.sleep(5)
            continue

        try:
            now_utc = datetime.now(timezone.utc)
            data = scraper.fetch(query)
            if data.get("status") == "ok":
                # 新着判定の基準時刻
                # ・初回: 過去24時間以内の記事のみ
                # ・2回目以降: 前回フェッチ時刻より新しい記事のみ
                if last_fetch_time is None:
                    cutoff = now_utc - timedelta(hours=24)
                else:
                    cutoff = last_fetch_time

                new_items = []
                for item in data.get("items", []):
                    url = item.get("url", "")
                    pub_dt: Optional[datetime] = item.pop("_pub_dt", None)  # 内部フィールドを除去

                    # URLが既読なら常にスキップ（重複排除）
                    if not url or url in seen_urls:
                        seen_urls.add(url)  # 念のため既読に登録
                        continue

                    # pubDate がある場合は基準時刻より古ければスキップ
                    if pub_dt is not None and pub_dt <= cutoff:
                        seen_urls.add(url)  # 古い記事も既読に登録して再出現を防ぐ
                        continue

                    # 新着記事として追加
                    seen_urls.add(url)
                    new_items.append(item)

                last_fetch_time = now_utc  # フェッチ完了時刻を更新

                if new_items:
                    send_data = dict(data)
                    send_data["items"] = new_items
                    send_data["total_items"] = len(new_items)
                    send_data["summary"] = f"{len(new_items)}件の新着ニュース"
                    _last_news_cache[query] = send_data
                    event_manager.broadcast(query, "result", send_data)
                    event_manager.broadcast(query, "log", {"message": f"{len(new_items)}件の新着記事を取得しました"})
        except Exception:
            pass

        time.sleep(60)

# --- Flask Routes ---

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/suggest")
def api_suggest():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])

    global _df_stocks
    if _df_stocks is not None and not _df_stocks.empty:
        code_col = _df_stocks.columns[1]
        name_col = _df_stocks.columns[2]
        
        q_norm = normalize(query)
        matches = _df_stocks[
            _df_stocks['_norm_name'].str.contains(q_norm, na=False, regex=False) |
            _df_stocks['_norm_code'].str.contains(q_norm, na=False, regex=False)
        ].head(20)
        
        results = []
        for _, row in matches.iterrows():
            results.append({
                "code": str(row[code_col]),
                "name": str(row[name_col])
            })
        return jsonify(results)
    
    return jsonify([])

@app.route("/search-stream")
def search_stream():
    query = request.args.get("q", "").strip()
    
    def generate():
        if not query:
            yield f"event: error\ndata: {json.dumps({'message': 'query is empty'})}\n\n"
            return
            
        yield f"event: start\ndata: {json.dumps({'query': query})}\n\n"
        yield ":" + " " * 2048 + "\n\n"
        
        q = queue.Queue(maxsize=100)
        event_manager.subscribe(query, q)
        registry.ensure_monitors(query)
        
        yield f"event: log\ndata: {json.dumps({'message': f'Subscribed to real-time events for {query}'})}\n\n"

        # 新規接続クライアントにキャッシュ済みの最新ニュースを即送信
        if query in _last_news_cache:
            cached = _last_news_cache[query]
            yield f"event: result\ndata: {json.dumps(cached, ensure_ascii=False)}\n\n"

        try:
            while True:
                try:
                    msg = q.get(timeout=15) # Heartbeat every 15s
                    yield msg
                except queue.Empty:
                    yield f"event: heartbeat\ndata: {json.dumps({'timestamp': now_iso()})}\n\n"
        except GeneratorExit:
            pass
        finally:
            event_manager.unsubscribe(query, q)
            print(f"Client disconnected from {query}")

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )

if __name__ == "__main__":
    app.run(debug=True, port=5050, threaded=True)
