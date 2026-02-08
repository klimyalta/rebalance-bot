#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Smart Spot Rebalance Bot — server.py (multi-asset + Telegram from config only + presets + WS + CSV)
- Портфели: 2-актива (BTC/USDT 50/50) и 3-актива (BTC 30% / ETH 30% / USDT 40%)
- Авто-режим с интервалом проверок
- Персистентность состояния
- Экспорт CSV
- WebSocket статус-поток
- Telegram-уведомления: токен и chat_id задаются ТОЛЬКО через config (из интерфейса), без хардкода
- Конфиг обновляем через /api/config (включая Telegram-поля)
- Переключение портфеля через /api/portfolio
- Тестовый эндпоинт /api/test_telegram
- Dry-run (без реальных ордеров), антиспам для Telegram

Примечания:
- Торгуем спотовыми парами ASSET/USDT (BTC/USDT, ETH/USDT).
- Комиссия учитывается грубо (fee_pct * value_usdt).
"""
import asyncio
import csv
import json
import logging
import os
import threading
from time import time
from typing import Dict, Any, Optional, List

import ccxt
import requests
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

# ===== Логирование =====
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ===== Константы =====
PERSIST_PATH = os.path.join(os.getcwd(), "state_store.json")
EXPORT_CSV_PATH = os.path.join(os.getcwd(), "rebalance_export.csv")
_save_lock = threading.Lock()

# ===== Приложение =====
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# ===== Глобальное состояние =====
state: Dict[str, Any] = {
    "api_key": None,
    "api_secret": None,
    "symbol_base": "USDT",   # quote currency
    "exchange": None,
    "running": False,
    "price": {},             # e.g. {"BTC/USDT": 60000.0, "ETH/USDT": 4000.0}
    "balances": {"BTC": 0.0, "ETH": 0.0, "USDT": 0.0},
    "history": [],           # {id, asset, pair, action, price, qty, value_usdt, fee_used, ts}
    "analytics": {
        "total_rebalances": 0,
        "total_fee": 0.0,
        "roi_pct": 0.0,
    },
    "config": {
        # portfolio
        "portfolio_mode": "50_50",
        "portfolio_presets": {
            "50_50": {"BTC": 0.50, "USDT": 0.50},
            "btc_eth_usdt_30_40_30": {"BTC": 0.30, "ETH": 0.30, "USDT": 0.40},
        },
        # strategy
        "drift_band": 0.04,
        "min_trade_value": 75.0,
        "slippage_limit_pct": 0.005,
        "max_rebalance_fraction": 0.25,
        "fee_pct": 0.001,
        "auto_mode": True,
        "interval_sec": 60,
        "initial_deposit": 0.0,
        # Telegram settings — заполняются только через UI/API
        "telegram_bot_token": "",
        "telegram_chat_id": "",
        "telegram_notify": True,
        # доп. флаги
        "dry_run": False,               # не отправлять реальные ордера, только логировать
        "telegram_rate_limit_sec": 5,  # антиспам: минимум секунд между сообщениями
    },
    "last_ref_price": {},    # per pair
    "_last_telegram_ts": 0,  # для антиспама
}

# ===== Персистентность =====
def save_state():
    payload = {
        "config": state["config"],
        "balances": state["balances"],
        "history": state["history"],
        "analytics": state["analytics"],
        "last_ref_price": state["last_ref_price"],
        "price": state["price"],
    }
    tmp = PERSIST_PATH + ".tmp"
    with _save_lock:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, PERSIST_PATH)
    logging.info(f"Состояние сохранено: {PERSIST_PATH}")

def load_state():
    if not os.path.exists(PERSIST_PATH):
        logging.info("Файл состояния не найден — старт без восстановления.")
        return
    try:
        with open(PERSIST_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        for k in ["config", "balances", "history", "analytics", "last_ref_price", "price"]:
            if k in data:
                state[k] = data[k]
        logging.info(f"Состояние восстановлено: {len(state['history'])} записей истории")
    except Exception as e:
        logging.warning(f"Ошибка загрузки состояния: {e}")

# ===== Telegram =====
def send_telegram_message(text: str):
    try:
        # антиспам
        now_ts = time()
        min_gap = float(state["config"].get("telegram_rate_limit_sec", 5) or 0)
        if now_ts - float(state.get("_last_telegram_ts", 0) or 0) < min_gap:
            return

        token = (state["config"].get("telegram_bot_token") or "").strip()
        chat_id = (state["config"].get("telegram_chat_id") or "").strip()
        notify = bool(state["config"].get("telegram_notify", True))
        if not notify or not token or not chat_id:
            return

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text}
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logging.warning(f"Telegram error: {r.status_code} {r.text}")
        else:
            state["_last_telegram_ts"] = now_ts
    except Exception as e:
        logging.warning(f"Ошибка отправки в Telegram: {e}")

# ===== Хелперы =====
def gen_id() -> str:
    return f"r{int(time()*1000)}"

def fetch_price(ex, pair: str) -> float:
    try:
        t = ex.fetch_ticker(pair)
        return float(t.get("last") or t.get("close") or 0.0)
    except Exception as e:
        logging.warning(f"Не удалось получить цену для {pair}: {e}")
        return 0.0

def get_balances(ex) -> Dict[str, float]:
    try:
        bal = ex.fetch_balance()
        total = bal.get('total', {})
        return {
            'BTC': float(total.get('BTC', 0) or 0.0),
            'ETH': float(total.get('ETH', 0) or 0.0),
            'USDT': float(total.get('USDT', 0) or 0.0),
        }
    except Exception as e:
        logging.warning(f"Не удалось получить балансы: {e}")
        return {'BTC': 0.0, 'ETH': 0.0, 'USDT': 0.0}

def compute_values_and_weights(prices: Dict[str, float], balances: Dict[str, float], preset: Dict[str, float]) -> Dict[str, Any]:
    val_by_asset = {}
    total = 0.0
    for asset, bal in balances.items():
        if asset == "USDT":
            v = bal
        else:
            pair = f"{asset}/USDT"
            price = prices.get(pair, 0.0)
            v = bal * price
        val_by_asset[asset] = v
        total += v
    weights = {a: (v / total if total > 0 else 0.0) for a, v in val_by_asset.items()}
    return {"val_by_asset": val_by_asset, "total": total, "weights": weights}

def need_rebalance_asset(current_w: float, target_w: float, band: float) -> bool:
    return abs(current_w - target_w) >= band

def bounded_buy_amount(delta_v: float, available_usdt: float, max_fraction: float) -> float:
    max_spend = available_usdt * max_fraction
    return max(0.0, min(delta_v, max_spend))

def calc_trades_for_preset(prices: Dict[str, float], balances: Dict[str, float], preset: Dict[str, float], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Возвращает список требуемых торговых действий:
    Каждый элемент: {asset, pair, action('buy'/'sell'), qty, value_usdt}
    """
    res = []
    info = compute_values_and_weights(prices, balances, preset)
    val_by_asset = info["val_by_asset"]
    total = info["total"]
    current_weights = info["weights"]

    fee = cfg["fee_pct"]
    min_value = cfg["min_trade_value"]
    max_fraction = cfg["max_rebalance_fraction"]
    band = cfg["drift_band"]

    for asset, target_w in preset.items():
        cur_w = current_weights.get(asset, 0.0)
        if not need_rebalance_asset(cur_w, target_w, band):
            continue
        target_v = target_w * total
        cur_v = val_by_asset.get(asset, 0.0)
        delta_v = target_v - cur_v  # >0 buy, <0 sell

        if asset == "USDT":
            # USDT достигается косвенно (через сделки других активов)
            continue

        pair = f"{asset}/USDT"
        price = prices.get(pair, 0.0)
        if price <= 0:
            continue

        if delta_v > 0:
            bounded = bounded_buy_amount(delta_v, val_by_asset.get("USDT", 0.0), max_fraction)
            if bounded < min_value:
                continue
            qty = bounded / price
            qty_net = qty * (1.0 - fee)
            res.append({"asset": asset, "pair": pair, "action": "buy", "qty": qty_net, "value_usdt": bounded})
        else:
            sell_value = abs(delta_v)
            if sell_value < min_value:
                continue
            qty = sell_value / price
            available_qty = balances.get(asset, 0.0)
            qty = min(qty, available_qty)
            if qty * price < min_value:
                continue
            qty_net = qty * (1.0 - fee)
            res.append({"asset": asset, "pair": pair, "action": "sell", "qty": qty_net, "value_usdt": qty * price})
    return res

def update_analytics(fee_used: float):
    a = state["analytics"]
    a["total_rebalances"] += 1
    a["total_fee"] = float(a.get("total_fee", 0.0)) + float(fee_used)
    dep = float(state["config"].get("initial_deposit", 0.0) or 0.0)
    a["roi_pct"] = (0.0 - a["total_fee"]) / dep * 100.0 if dep > 0 else 0.0

# ===== Основной проход ребаланса =====
async def rebalance_once() -> Dict[str, Any]:
    if state.get("exchange") is None and not state["config"].get("dry_run", False):
        return {"status": "error", "message": "Нет подключения. Вызовите /api/connect."}

    ex = state["exchange"]
    cfg = state["config"]
    preset_name = cfg.get("portfolio_mode")
    preset = cfg.get("portfolio_presets", {}).get(preset_name, {})

    # Обновляем цены для всех пар, и балансы
    pairs = [f"{a}/USDT" for a in preset.keys() if a != "USDT"]
    prices = {}
    if ex:
        for p in pairs:
            prices[p] = fetch_price(ex, p)
    else:
        for p in pairs:
            prices[p] = state["price"].get(p, 0.0)
    state["price"].update(prices)

    if ex:
        bals = get_balances(ex)
        state["balances"].update(bals)

    # Compute trades
    trades = calc_trades_for_preset(state["price"], state["balances"], preset, cfg)
    if not trades:
        info = compute_values_and_weights(state["price"], state["balances"], preset)
        return {
            "status": "ok",
            "message": "Ребаланс не требуется.",
            "prices": state["price"],
            "val_by_asset": info["val_by_asset"],
            "total": info["total"],
            "weights": info["weights"],
            "config": cfg,
            "balances": state["balances"],
            "history": state["history"][-50:],
            "analytics": state["analytics"],
        }

    # Execute trades sequentially (or simulate if dry_run)
    executed = []
    total_fee_used = 0.0
    for t in trades:
        pair = t["pair"]
        price = state["price"].get(pair)
        if not price or price <= 0:
            if ex:
                price = fetch_price(ex, pair)
                state["price"][pair] = price
            else:
                price = 0.0

        # slippage check
        ref = state["last_ref_price"].get(pair) or price
        slip = abs(price - ref) / max(ref, 1e-9)
        if slip > cfg["slippage_limit_pct"]:
            msg = f"Слиппедж для {pair} {slip*100:.2f}% > лимита — пропуск ордера {t}"
            logging.warning(msg)
            send_telegram_message(msg)
            continue

        if cfg.get("dry_run", False):
            # симулируем сделку
            fee_used = cfg["fee_pct"] * t["value_usdt"]
            hist = {
                "id": gen_id(),
                "asset": t["asset"],
                "pair": pair,
                "action": t["action"],
                "price": price,
                "qty": t["qty"],
                "value_usdt": t["value_usdt"],
                "fee_used": fee_used,
                "ts": int(time())
            }
            state["history"].append(hist)
            executed.append(hist)
            state["last_ref_price"][pair] = price
            # обновляем балансы грубо (симуляция)
            if t["action"] == "buy":
                state["balances"][t["asset"]] = float(state["balances"].get(t["asset"], 0.0)) + t["qty"]
                state["balances"]["USDT"] = float(state["balances"].get("USDT", 0.0)) - t["value_usdt"]
            else:
                state["balances"][t["asset"]] = max(0.0, float(state["balances"].get(t["asset"], 0.0)) - t["qty"])
                state["balances"]["USDT"] = float(state["balances"].get("USDT", 0.0)) + t["value_usdt"]
            send_telegram_message(
                f"[DRY] Ребаланс: {t['action'].upper()} {t['asset']} | {pair}\n"
                f"Qty: {t['qty']:.6f} @ ~{price:.2f}\n"
                f"Value: {t['value_usdt']:.2f} USDT"
            )
            total_fee_used += fee_used
            continue

        # реальный ордер
        try:
            order = ex.create_order(pair, "market", t["action"], t["qty"])
            logging.info(f"ORDER {t['action'].upper()} {pair} qty={t['qty']:.6f} @ ~{price:.2f}: {order}")
        except Exception as e:
            msg = f"Ошибка MARKET ордера для {pair}: {e}"
            logging.exception(msg)
            send_telegram_message(msg)
            continue

        fee_used = cfg["fee_pct"] * t["value_usdt"]
        total_fee_used += fee_used
        hist = {
            "id": gen_id(),
            "asset": t["asset"],
            "pair": pair,
            "action": t["action"],
            "price": price,
            "qty": t["qty"],
            "value_usdt": t["value_usdt"],
            "fee_used": fee_used,
            "ts": int(time())
        }
        state["history"].append(hist)
        executed.append(hist)
        state["last_ref_price"][pair] = price

        send_telegram_message(
            f"Ребаланс: {t['action'].upper()} {t['asset']} | {pair}\n"
            f"Qty: {t['qty']:.6f} @ ~{price:.2f}\n"
            f"Value: {t['value_usdt']:.2f} USDT | Fee≈{fee_used:.4f} USDT"
        )

        # обновить балансы после сделки (best-effort)
        bals2 = get_balances(ex)
        state["balances"].update(bals2)

    if total_fee_used > 0:
        update_analytics(total_fee_used)
    save_state()

    info_after = compute_values_and_weights(state["price"], state["balances"], preset)
    return {
        "status": "filled" if executed else "partial",
        "message": f"Выполнено ордеров: {len(executed)}",
        "executed": executed,
        "prices": state["price"],
        "val_by_asset": info_after["val_by_asset"],
        "total": info_after["total"],
        "weights_after": info_after["weights"],
        "balances": state["balances"],
        "history": state["history"][-50:],
        "analytics": state["analytics"],
    }

# ===== Авто‑цикл =====
auto_task: Optional[asyncio.Task] = None

async def auto_loop():
    logging.info("Авто‑режим ребаланса запущен.")
    send_telegram_message("Авто‑режим ребаланса запущен.")
    while True:
        try:
            if state["config"].get("auto_mode", False) and (state.get("exchange") is not None or state["config"].get("dry_run", False)):
                await rebalance_once()
        except Exception as e:
            msg = f"Ошибка авто‑цикла: {e}"
            logging.exception(msg)
            send_telegram_message(msg)
        interval = int(state["config"].get("interval_sec", 60))
        await asyncio.sleep(max(10, interval))

@app.on_event("startup")
async def on_startup():
    global auto_task
    load_state()
    if auto_task is None:
        auto_task = asyncio.create_task(auto_loop())

# ===== API =====
@app.post("/api/connect")
async def connect(payload: Dict[str, Any]):
    try:
        state["api_key"] = payload.get("api_key")
        state["api_secret"] = payload.get("api_secret")

        if state["config"].get("dry_run", False):
            logging.info("DRY-RUN режим: пропуск реального подключения к бирже.")
        else:
            state["exchange"] = ccxt.bybit({
                'apiKey': state["api_key"],
                'secret': state["api_secret"],
                'enableRateLimit': True,
            })

            try:
                bal = state["exchange"].fetch_balance()
                quote = state["symbol_base"]
                initial_dep = float(bal.get('total', {}).get(quote, 0) or 0.0)
                state["config"]["initial_deposit"] = initial_dep
                logging.info(f"Начальный депозит: {initial_dep} {quote}")
            except Exception as e:
                logging.warning(f"Не удалось получить баланс для фиксации депозита: {e}")

            # первичные балансы и цены
            bals = get_balances(state["exchange"])
            state["balances"].update(bals)
            for asset in ["BTC", "ETH"]:
                pair = f"{asset}/USDT"
                state["price"][pair] = fetch_price(state["exchange"], pair)

        save_state()
        logging.info("Подключено (или подготовлено) и получены начальные данные.")
        send_telegram_message("Подключено к бирже. Бот готов к работе.")
        return {"status": "ok", "balances": state["balances"], "initial_deposit": state["config"]["initial_deposit"]}
    except Exception as e:
        msg = f"Ошибка подключения: {e}"
        logging.exception(msg)
        send_telegram_message(msg)
        return {"status": "error", "message": str(e)}

@app.post("/api/config")
async def update_config(payload: Dict[str, Any]):
    try:
        cfg = state["config"]
        for k in ["drift_band","min_trade_value","slippage_limit_pct",
                  "max_rebalance_fraction","fee_pct","auto_mode","interval_sec",
                  "initial_deposit","telegram_notify","telegram_bot_token","telegram_chat_id",
                  "portfolio_mode","dry_run","telegram_rate_limit_sec"]:
            if k in payload:
                v = payload[k]
                if isinstance(v, (int, float)):
                    cfg[k] = float(v)
                elif isinstance(v, bool):
                    cfg[k] = bool(v)
                elif isinstance(v, str) and v.lower() in ["true","false"]:
                    cfg[k] = (v.lower() == "true")
                else:
                    cfg[k] = v

        # validate portfolio_mode if provided
        mode = cfg.get("portfolio_mode")
        if mode and mode not in cfg.get("portfolio_presets", {}):
            return {"status": "error", "message": f"Unknown portfolio_mode {mode}"}

        save_state()
        logging.info(f"Конфиг обновлён: {cfg}")
        send_telegram_message("Конфиг обновлён.")
        return {"status": "ok", "config": cfg}
    except Exception as e:
        msg = f"Ошибка обновления конфига: {e}"
        logging.exception(msg)
        send_telegram_message(msg)
        return {"status": "error", "message": str(e)}

@app.post("/api/start")
async def start_once(payload: Dict[str, Any]):
    try:
        res = await rebalance_once()
        state["running"] = state["config"].get("auto_mode", False)
        return res
    except Exception as e:
        msg = f"Ошибка /api/start: {e}"
        logging.exception(msg)
        send_telegram_message(msg)
        return {"status": "error", "message": str(e)}

@app.post("/api/stop")
async def stop_auto():
    try:
        state["config"]["auto_mode"] = False
        state["running"] = False
        save_state()
        msg = "Авто‑режим остановлен."
        logging.info(msg)
        send_telegram_message(msg)
        return {"status": "stopped"}
    except Exception as e:
        msg = f"Ошибка /api/stop: {e}"
        logging.exception(msg)
        send_telegram_message(msg)
        return {"status": "error", "message": str(e)}

@app.post("/api/portfolio")
async def set_portfolio(payload: Dict[str, Any]):
    """
    Установить режим портфеля:
    { "mode": "50_50" } или { "mode": "btc_eth_usdt_30_40_30" }
    """
    try:
        mode = payload.get("mode")
        presets = state["config"].get("portfolio_presets", {})
        if mode not in presets:
            return {"status": "error", "message": f"Unknown mode {mode}"}
        state["config"]["portfolio_mode"] = mode
        save_state()
        logging.info(f"Портфель переключён на {mode}")
        send_telegram_message(f"Портфель переключён на режим: {mode}")
        return {"status": "ok", "mode": mode, "preset": presets[mode]}
    except Exception as e:
        msg = f"Ошибка переключения портфеля: {e}"
        logging.exception(msg)
        send_telegram_message(msg)
        return {"status": "error", "message": str(e)}

@app.post("/api/test_telegram")
async def test_telegram(payload: Dict[str, Any]):
    try:
        text = payload.get("text") or "Тестовое сообщение от Rebalance Bot"
        send_telegram_message(text)
        return {"status": "ok"}
    except Exception as e:
        logging.exception(f"Ошибка теста Telegram: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/status")
async def status():
    price_map = state.get("price", {})
    balances = state.get("balances", {})
    preset_name = state["config"].get("portfolio_mode")
    preset = state["config"].get("portfolio_presets", {}).get(preset_name, {})
    info = compute_values_and_weights(price_map, balances, preset)
    return {
        "state": "running" if state.get("config", {}).get("auto_mode") else "stopped",
        "portfolio_mode": preset_name,
        "preset": preset,
        "prices": price_map,
        "balances": balances,
        "val_by_asset": info["val_by_asset"],
        "val_total": info["total"],
        "weights": info["weights"],
        "config": state["config"],
        "last_ref_price": state["last_ref_price"],
        "history": state["history"][-50:],
        "analytics": state["analytics"],
    }

# ===== Экспорт CSV =====
@app.get("/api/export")
async def export_history():
    try:
        with open(EXPORT_CSV_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["id","asset","pair","action","price","qty","value_usdt","fee_used","ts"])
            for h in state["history"]:
                w.writerow([
                    h.get("id"), h.get("asset"), h.get("pair"), h.get("action"),
                    h.get("price"), h.get("qty"), h.get("value_usdt"), h.get("fee_used"), h.get("ts")
                ])
        logging.info(f"Экспортирован CSV: {EXPORT_CSV_PATH}")
        return FileResponse(EXPORT_CSV_PATH, media_type="text/csv", filename=os.path.basename(EXPORT_CSV_PATH))
    except Exception as e:
        msg = f"Ошибка экспорта CSV: {e}"
        logging.exception(msg)
        send_telegram_message(msg)
        return {"status": "error", "message": str(e)}

# ===== WebSocket =====
@app.websocket("/ws")
async def ws_stream(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            preset_name = state["config"].get("portfolio_mode")
            preset = state["config"].get("portfolio_presets", {}).get(preset_name, {})
            info = compute_values_and_weights(state.get("price", {}), state.get("balances", {}), preset)
            msg = {
                "type": "status",
                "state": "running" if state.get("config", {}).get("auto_mode") else "stopped",
                "portfolio_mode": preset_name,
                "preset": preset,
                "prices": state.get("price", {}),
                "balances": state.get("balances", {}),
                "val_by_asset": info["val_by_asset"],
                "val_total": info["total"],
                "weights": info["weights"],
                "config": state["config"],
                "history": state["history"][-50:],
                "analytics": state["analytics"],
            }
            await ws.send_text(json.dumps(msg))
            await asyncio.sleep(3)
    except Exception as e:
        logging.warning(f"WS закрыт: {e}")
    finally:
        try:
            await ws.close()
        except Exception:
            pass

# ===== Запуск =====
if __name__ == "__main__":
    import uvicorn
    logging.info("Старт Smart Spot Rebalance Bot — multi-asset + Telegram(config only) + presets + WS + CSV + dry-run")
    uvicorn.run(app, host="0.0.0.0", port=8000)
