
import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI

from .pricing import get_prices, format_prices


# CONFIG
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("nft-radar")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")
ADMIN_ID = os.getenv("ADMIN_ID", "")

POLL_SECONDS = max(15, int(os.getenv("POLL_SECONDS", "25")))

FEED_URL = "https://api.changes.tg/changes"
TELEGRAM_URL = (
    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    if BOT_TOKEN else ""
)

# STATE
state = {
    "running": False,
    "last_scan": None,
    "last_error": None,
    "events": 0,
    "notified": 0,
    "failed_notifications": 0,
}

cursor = None
seen = set()
MAX_SEEN = 2000


def get_recipients():
    return list(dict.fromkeys(
        str(x).strip()
        for x in (CHAT_ID, ADMIN_ID)
        if str(x).strip()
    ))


def value_name(value):
    if isinstance(value, dict):
        return (
            value.get("name")
            or value.get("title")
            or value.get("model")
            or str(value)
        )
    return str(value) if value is not None else None


def event_label(kind):
    labels = {
        "gift.added": "Новый подарок",
        "gift.upgrades_added": "Добавлены улучшения",
        "crafts.added": "Новые крафты",
        "model.added": "Новая модель",
        "model.renamed": "Модель переименована",
        "model.redrawn": "Модель перерисована",
        "model.reclassified": "Изменена классификация модели",
        "model.tier_changed": "Изменён уровень модели",
        "model.rarity_changed": "Изменена редкость модели",
        "symbol.added": "Новый узор",
        "symbol.rarity_changed": "Изменена редкость узора",
        "backdrop.rarity_changed": "Изменена редкость фона",
    }
    return labels.get(kind, kind)


async def notify(message):
    recipients = get_recipients()

    if not BOT_TOKEN or not recipients:
        log.error("Telegram credentials or recipients missing")
        return False

    success = True

    async with httpx.AsyncClient(timeout=20) as client:
        for recipient in recipients:
            try:
                response = await client.post(
                    TELEGRAM_URL,
                    json={
                        "chat_id": recipient,
                        "text": message[:4000],
                        "disable_web_page_preview": True,
                    },
                )
                response.raise_for_status()

                result = response.json()

                if not result.get("ok"):
                    success = False
                    log.error(
                        "Telegram API rejected message for %s: %s",
                        recipient,
                        result,
                    )

            except Exception:
                success = False
                log.exception(
                    "Notification failed for recipient %s",
                    recipient,
                )

    return success


async def build_message(event):
    gift = value_name(event.get("gift")) or "Неизвестный подарок"
    model = value_name(event.get("model"))
    backdrop = value_name(event.get("backdrop"))
    symbol = value_name(event.get("symbol"))

    kind = event.get("type", "update")

    lines = [
        "🎁 NFT RADAR",
        "",
        f"Событие: {event_label(kind)}",
        f"Подарок: {gift}",
    ]

    if model:
        lines.append(f"Модель: {model}")

    if backdrop:
        lines.append(f"Фон: {backdrop}")

    if symbol:
        lines.append(f"Узор: {symbol}")

    if "fromPermille" in event and "toPermille" in event:
        try:
            old = float(event["fromPermille"]) / 10
            new = float(event["toPermille"]) / 10
            lines.append(f"Редкость: {old:g}% → {new:g}%")
        except (TypeError, ValueError):
            pass

    if event.get("giftId"):
        lines.append(f"Gift ID: {event['giftId']}")

    if event.get("giftId"):
        lines.append(
            f"Telegram: https://t.me/nft/{event['giftId']}"
        )

    # Prices
    try:
        prices = await get_prices(gift, model)
        price_text = format_prices(prices)
    except Exception:
        log.exception("Price lookup failed for %s / %s", gift, model)
        price_text = "Цены: временно недоступны"

    lines.extend(["", price_text])

    event_time = event.get("at", "—")
    lines.extend([
        "",
        f"Время события: {event_time}",
        f"ID события: {event.get('id', '—')}",
        "Источник: GiftChanges API",
    ])

    return "\n".join(lines)


async def scan():
    global cursor

    initial_cursor = int(time.time() * 1000)

    params = {"limit": 100}
    params["since"] = cursor if cursor is not None else initial_cursor

    async with httpx.AsyncClient(timeout=25) as client:
        response = await client.get(FEED_URL, params=params)
        response.raise_for_status()
        data = response.json()

    if isinstance(data, list):
        events = data
        next_cursor = None
    else:
        events = data.get("events", data.get("changes", []))
        next_cursor = data.get("next")

    if not isinstance(events, list):
        raise ValueError("Unexpected GiftChanges response format")

    for event in events:
        if not isinstance(event, dict):
            continue

        event_id = str(event.get("id", ""))

        if not event_id or event_id in seen:
            continue

        message = await build_message(event)

        delivered = await notify(message)

        if not delivered:
            state["failed_notifications"] += 1
            raise RuntimeError(
                f"Notification delivery failed for event {event_id}"
            )

        seen.add(event_id)

        if len(seen) > MAX_SEEN:
            seen.clear()
            seen.add(event_id)

        state["events"] += 1
        state["notified"] += 1

        log.info("Event delivered: %s", event_id)

    if next_cursor is not None:
        cursor = next_cursor
    elif cursor is None:
        cursor = initial_cursor

    state["last_scan"] = datetime.now(timezone.utc).isoformat()


async def scanner():
    state["running"] = True
    log.info("NFT-RADAR scanner started")

    while True:
        try:
            await scan()
            state["last_error"] = None

        except asyncio.CancelledError:
            raise

        except Exception as error:
            state["last_error"] = str(error)
            log.exception("Scanner error")

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


app = FastAPI(
    title="NFT-RADAR",
    version="2.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def home():
    return {
        "name": "NFT-RADAR",
        "version": "2.0.0",
        "status": "online",
    }


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/status")
async def status():
    return {
        **state,
        "cursor": cursor,
        "poll_seconds": POLL_SECONDS,
        "telegram_configured": bool(BOT_TOKEN and get_recipients()),
        "client_configured": bool(CHAT_ID),
        "admin_configured": bool(ADMIN_ID),
        "seen_events": len(seen),
    }


@app.post("/test")
async def test():
    delivered = await notify(
        "NFT-RADAR v2.0\n\n"
        "Тестовое уведомление.\n"
        "Канал связи работает."
    )

    return {
        "sent": delivered,
        "recipients": len(get_recipients()),
    }
