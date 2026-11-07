import asyncio
import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("nft-radar")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "25"))

FEED_URL = "https://api.changes.tg/changes"

state = {
    "running": False,
    "last_scan": None,
    "last_error": None,
    "events": 0
}

seen = set()


async def notify(message):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram not configured")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(url, json={
            "chat_id": CHAT_ID,
            "text": message,
            "disable_web_page_preview": True
        })
        response.raise_for_status()


def extract_events(data):
    if isinstance(data, list):
        return data, None

    if isinstance(data, dict):
        events = (
            data.get("changes")
            or data.get("events")
            or data.get("data")
            or []
        )
        return events, data.get("next")

    return [], None


async def scan():
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.get(FEED_URL)
        response.raise_for_status()
        data = response.json()

    events, cursor = extract_events(data)

    for event in events:
        if not isinstance(event, dict):
            continue

        event_id = str(event.get("id") or repr(event))

        if event_id in seen:
            continue

        seen.add(event_id)

        kind = event.get("type", "update")
        gift = event.get("gift") or event.get("giftName") or "Unknown"
        model = event.get("model") or event.get("modelName") or "—"

        message = (
            "🎁 NFT RADAR\n\n"
            f"Событие: {kind}\n"
            f"Подарок: {gift}\n"
            f"Модель: {model}\n"
            f"Время: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
        )

        await notify(message)
        state["events"] += 1

    state["last_scan"] = datetime.now(timezone.utc).isoformat()


async def scanner():
    state["running"] = True

    while True:
        try:
            await scan()
            state["last_error"] = None
        except Exception as error:
            state["last_error"] = str(error)
            log.exception("Scanner error")

        await asyncio.sleep(max(10, POLL_SECONDS))


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(scanner())
    yield
    task.cancel()


app = FastAPI(title="NFT-RADAR", lifespan=lifespan)


@app.get("/")
async def home():
    return {
        "name": "NFT-RADAR",
        "status": "online"
    }


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/status")
async def status():
    return state


@app.post("/test")
async def test():
    await notify("NFT-RADAR: тестовое уведомление.")
    return {"sent": True}