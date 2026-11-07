import asyncio
import os
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI

from .pricing import get_prices, format_prices

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("nft-radar")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")
ADMIN_ID = os.getenv("ADMIN_ID", "")
POLL_SECONDS = max(15, int(os.getenv("POLL_SECONDS", "25")))

FEED_URL = "https://api.changes.tg/changes"

state = {
    "running": False,
    "last_scan": None,
    "last_error": None,
    "events": 0,
    "notified": 0
}

cursor = None
seen = set()


async def notify(message):
    if not BOT_TOKEN:
        log.warning("BOT_TOKEN missing")
        return

    recipients = list(dict.fromkeys(
        x for x in [CHAT_ID, ADMIN_ID] if x
    ))

    if not recipients:
        log.warning("No recipients configured")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    async with httpx.AsyncClient(timeout=20) as client:
        for chat_id in recipients:
            try:
                r = await client.post(url, json={
                    "chat_id": chat_id,
                    "text": message[:4000],
                    "disable_web_page_preview": True
                })
                r.raise_for_status()

                result = r.json()
                if not result.get("ok"):
                    log.error("Telegram rejected message: %s", result)
            except Exception:
                log.exception("Failed to notify chat %s", chat_id)

async def scan():
    global cursor

    params = {"limit": 100}

    if cursor:
        params["since"] = cursor
    else:
        # При первом запуске не отправляем всю старую историю.
        params["since"] = int(time.time() * 1000)

    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.get(FEED_URL, params=params)
        r.raise_for_status()
        data = r.json()

    events = (
    data if isinstance(data, list)
    else data.get("events", data.get("changes", []))
)
    next_cursor = None if isinstance(data, list) else data.get("next")

    for event in events:
        if not isinstance(event, dict):
            continue

        event_id = str(event.get("id", repr(event)))

        if event_id in seen:
            continue

        seen.add(event_id)

        kind = event.get("type", "update")
        gift = event.get("gift", "Unknown")

        model = event.get("model")
        if isinstance(model, dict):
            model = model.get("name")

        backdrop = event.get("backdrop")
        symbol = event.get("symbol")

        lines = [
            "🎁 NFT RADAR",
            "",
            f"Событие: {kind}",
            f"Подарок: {gift}"
        ]

        if model:
            lines.append(f"Модель: {model}")
        if backdrop:
            lines.append(f"Фон: {backdrop}")
        if symbol:
            lines.append(f"Узор: {symbol}")
        if "fromPermille" in event and "toPermille" in event:
    old = event["fromPermille"] / 10
    new = event["toPermille"] / 10
    lines.append(f"Редкость: {old}% → {new}%")

        if event.get("giftId"):
    lines.append(f"Gift ID: {event['giftId']}")

        if model:
            prices = await get_prices(gift, model)
        else:
            prices = await get_prices(gift)

        lines.extend(["", format_prices(prices)])
        lines.extend([
            "",
            f"Время: {event.get('at', '—')}",
            "Источник событий: GiftChanges API"
        ])

        await notify("\n".join(lines))

        state["events"] += 1
        state["notified"] += 1

    if next_cursor is not None:
        cursor = next_cursor

    state["last_scan"] = datetime.now(timezone.utc).isoformat()


async def scanner():
    state["running"] = True

    while True:
        try:
            await scan()
            state["last_error"] = None
        except Exception as e:
            state["last_error"] = str(e)
            log.exception("Scanner failed")

        await asyncio.sleep(POLL_SECONDS)


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(scanner())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="NFT-RADAR", lifespan=lifespan)


@app.get("/")
async def home():
    return {"name": "NFT-RADAR", "status": "online"}


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/status")
async def status():
    return {
        **state,
        "cursor": cursor,
        "poll_seconds": POLL_SECONDS,
        "telegram_configured": bool(BOT_TOKEN and CHAT_ID)
    }


@app.post("/test")
async def test():
    await notify("NFT-RADAR: тестовое уведомление.")
    return {"sent": True}