import httpx
from urllib.parse import quote

BASE = "https://starprice.net/api/v1/gifts"

async def get_prices(gift, model=None):
    path = quote(str(gift), safe="")
    if model:
        path += "/" + quote(str(model), safe="")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(f"{BASE}/{path}")
            r.raise_for_status()
            return r.json()
    except Exception as e:
        return {"error": str(e)}

def format_prices(data):
    if data.get("error"):
        return "Цены: временно недоступны"

    item = data.get("model") or data
    floor = item.get("floor")
    offers = item.get("offers") or []

    lines = []

    if floor:
        lines.append(
            f"Floor: {floor.get('price')} {floor.get('currency')} "
            f"(≈ {floor.get('rub')} ₽)"
        )
    else:
        lines.append("Floor: нет данных")

    for offer in offers:
        market = offer.get("market", "?")
        price = offer.get("price", "?")
        currency = offer.get("currency", "")
        rub = offer.get("rub")
        url = offer.get("url")

        line = f"{market}: {price} {currency}"
        if rub is not None:
            line += f" (≈ {rub} ₽)"
        if url:
            line += f"\n{url}"

        lines.append(line)

    return "\n".join(lines)