"""
amoCRM Webhook Server — Welkin × Midea
Production server for Railway deployment.
Supports all 5 bots: Consultant, Warmer, Reactivator, Service, Referral.
Auth: Bearer JWT access_token (from env var AMO_ACCESS_TOKEN).
"""
import os
import time
import logging
import json as _json
from typing import Optional
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import requests
import uvicorn

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ─── amoCRM Config ────────────────────────────────────────────────────────────
AMO_DOMAIN = os.getenv("AMO_DOMAIN", "graverstudiouzb.amocrm.ru")
AMO_BASE   = f"https://{AMO_DOMAIN}"

PIPELINE_ID       = int(os.getenv("PIPELINE_ID",       "10785018"))
STATUS_NEW_LEAD   = int(os.getenv("STATUS_NEW_LEAD",   "84916158"))  # Новый лид (бот)
STATUS_WARM       = int(os.getenv("STATUS_WARM",       "84916162"))  # В прогреве
STATUS_REACTIVATE = int(os.getenv("STATUS_REACTIVATE", "84916166"))  # Реактивация
STATUS_WON        = int(os.getenv("STATUS_WON",        "142"))       # Успешно реализовано

FIELD_AREA        = int(os.getenv("FIELD_AREA",        "2897741"))
FIELD_BUDGET      = int(os.getenv("FIELD_BUDGET",      "2897743"))
FIELD_ROOM_TYPE   = int(os.getenv("FIELD_ROOM_TYPE",   "2897745"))
FIELD_SOURCE      = int(os.getenv("FIELD_SOURCE",      "2897747"))
FIELD_TELEGRAM_ID = int(os.getenv("FIELD_TELEGRAM_ID", "2897749"))

ROOM_TYPE_ENUMS = {
    "квартира": 5132395, "apartment": 5132395,
    "офис": 5132397,     "office": 5132397,
    "дом": 5132399,      "house": 5132399,
    "коммерческое": 5132401, "commercial": 5132401,
}
SOURCE_ENUMS = {
    "instagram": 5132403,
    "whatsapp": 5132405,
    "telegram": 5132407,
    "сайт": 5132409, "site": 5132409,
    "реферал": 5132411, "referral": 5132411,
}

# ─── Token management ─────────────────────────────────────────────────────────
_access_token: str = ""
_token_loaded_at: float = 0.0
TOKEN_TTL = 82800  # 23 hours (access_token lives 24h)


def get_access_token() -> str:
    """Return access token from env var. Reloads from env every TOKEN_TTL seconds."""
    global _access_token, _token_loaded_at
    now = time.time()
    if not _access_token or (now - _token_loaded_at) > TOKEN_TTL:
        token = os.getenv("AMO_ACCESS_TOKEN", "")
        if not token:
            raise RuntimeError("AMO_ACCESS_TOKEN env var is not set!")
        _access_token = token
        _token_loaded_at = now
        logger.info("Loaded AMO_ACCESS_TOKEN from env")
    return _access_token


def amo_headers() -> dict:
    return {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json"
    }


def test_token() -> bool:
    try:
        r = requests.get(f"{AMO_BASE}/api/v4/account", headers=amo_headers(), timeout=10)
        return r.status_code == 200
    except Exception:
        return False


# ─── amoCRM helpers ───────────────────────────────────────────────────────────

def find_enum_id(mapping: dict, value: str) -> Optional[int]:
    if not value:
        return None
    return mapping.get(str(value).lower().strip())


def find_or_create_contact(name: str, phone: str) -> int:
    """Find existing contact by phone or create new one."""
    if phone:
        r = requests.get(
            f"{AMO_BASE}/api/v4/contacts",
            params={"query": phone},
            headers=amo_headers(), timeout=15
        )
        if r.status_code == 200:
            contacts = r.json().get("_embedded", {}).get("contacts", [])
            if contacts:
                cid = contacts[0]["id"]
                logger.info(f"Found existing contact id={cid}")
                return cid

    contact_fields = []
    if phone:
        contact_fields.append({
            "field_code": "PHONE",
            "values": [{"value": phone, "enum_code": "WORK"}]
        })
    payload = [{"name": name, "custom_fields_values": contact_fields}]
    r = requests.post(f"{AMO_BASE}/api/v4/contacts", json=payload, headers=amo_headers(), timeout=15)
    r.raise_for_status()
    cid = r.json()["_embedded"]["contacts"][0]["id"]
    logger.info(f"Created contact id={cid}")
    return cid


def create_lead(name: str, contact_id: int, custom_fields: list,
                pipeline_id: int, status_id: int, tags: list = None) -> int:
    payload = [{
        "name": name,
        "pipeline_id": pipeline_id,
        "status_id": status_id,
        "custom_fields_values": custom_fields,
        "_embedded": {"contacts": [{"id": contact_id}]}
    }]
    if tags:
        payload[0]["_embedded"]["tags"] = [{"name": t} for t in tags]
    r = requests.post(f"{AMO_BASE}/api/v4/leads", json=payload, headers=amo_headers(), timeout=15)
    r.raise_for_status()
    lid = r.json()["_embedded"]["leads"][0]["id"]
    logger.info(f"Created lead id={lid}")
    return lid


def update_lead_status(lead_id: int, status_id: int) -> bool:
    r = requests.patch(
        f"{AMO_BASE}/api/v4/leads/{lead_id}",
        json={"status_id": status_id},
        headers=amo_headers(), timeout=15
    )
    return r.status_code in [200, 204]


def find_lead_by_contact(contact_id: int) -> Optional[int]:
    r = requests.get(
        f"{AMO_BASE}/api/v4/leads",
        params={"filter[contact_id]": contact_id, "order[id]": "desc", "limit": 1},
        headers=amo_headers(), timeout=15
    )
    if r.status_code == 200:
        leads = r.json().get("_embedded", {}).get("leads", [])
        if leads:
            return leads[0]["id"]
    return None


def add_note(lead_id: int, text: str):
    payload = [{"entity_id": lead_id, "note_type": "common", "params": {"text": text}}]
    r = requests.post(
        f"{AMO_BASE}/api/v4/leads/notes",
        json=payload, headers=amo_headers(), timeout=15
    )
    if r.status_code not in [200, 204]:
        logger.warning(f"Note failed: {r.status_code} {r.text[:200]}")


def add_tag(lead_id: int, tag: str):
    r = requests.patch(
        f"{AMO_BASE}/api/v4/leads/{lead_id}",
        json={"_embedded": {"tags": [{"name": tag}]}},
        headers=amo_headers(), timeout=15
    )
    return r.status_code in [200, 204]


def build_custom_fields(body: dict) -> list:
    fields = []
    area = body.get("area") or body.get("lead_area", "")
    if area:
        try:
            fields.append({"field_id": FIELD_AREA, "values": [{"value": float(str(area).replace(",", "."))}]})
        except (ValueError, TypeError):
            pass
    budget = body.get("budget") or body.get("lead_budget", "")
    if budget:
        try:
            fields.append({"field_id": FIELD_BUDGET, "values": [{"value": float(str(budget).replace(",", "").replace(" ", ""))}]})
        except (ValueError, TypeError):
            pass
    room_type = body.get("room_type") or body.get("lead_room_type", "")
    if room_type:
        eid = find_enum_id(ROOM_TYPE_ENUMS, str(room_type))
        if eid:
            fields.append({"field_id": FIELD_ROOM_TYPE, "values": [{"enum_id": eid}]})
    source = body.get("source", "")
    if source:
        eid = find_enum_id(SOURCE_ENUMS, str(source))
        if eid:
            fields.append({"field_id": FIELD_SOURCE, "values": [{"enum_id": eid}]})
    tg_id = str(body.get("telegram_id", ""))
    if tg_id:
        fields.append({"field_id": FIELD_TELEGRAM_ID, "values": [{"value": tg_id}]})
    return fields


def build_note(body: dict, bot_label: str = "") -> str:
    note = body.get("note") or body.get("lead_comment") or body.get("comment", "")
    if note:
        return f"{'🤖 ' + bot_label + chr(10) if bot_label else ''}{note}"
    parts = []
    if bot_label:        parts.append(f"🤖 {bot_label}")
    phone = body.get("phone") or body.get("lead_phone", "")
    if phone:            parts.append(f"📞 Телефон: {phone}")
    source = body.get("source", "")
    if source:           parts.append(f"📌 Источник: {source}")
    area = body.get("area") or body.get("lead_area", "")
    if area:             parts.append(f"📐 Площадь: {area} м²")
    room_type = body.get("room_type") or body.get("lead_room_type", "")
    if room_type:        parts.append(f"🏠 Тип: {room_type}")
    priority = body.get("priority", "")
    if priority:         parts.append(f"⚡ Приоритет: {priority}")
    sun_side = body.get("sun_side", "")
    if sun_side:         parts.append(f"☀️ Сторона: {sun_side}")
    budget = body.get("budget") or body.get("lead_budget", "")
    if budget:           parts.append(f"💰 Бюджет: {budget} сум")
    tg_id = body.get("telegram_id", "")
    if tg_id:            parts.append(f"🔗 Telegram ID: {tg_id}")
    return "\n".join(parts) if parts else "Лид от бота"


async def parse_body(request: Request) -> dict:
    ct = request.headers.get("content-type", "")
    raw = await request.body()
    try:
        if "application/json" in ct:
            return _json.loads(raw)
        elif "multipart/form-data" in ct:
            form = await request.form()
            return {k: v for k, v in form.items()}
        elif "application/x-www-form-urlencoded" in ct:
            from urllib.parse import parse_qs
            return {k: v[0] for k, v in parse_qs(raw.decode()).items()}
        else:
            try:
                return _json.loads(raw)
            except Exception:
                from urllib.parse import parse_qs
                return {k: v[0] for k, v in parse_qs(raw.decode()).items()}
    except Exception as e:
        logger.error(f"Body parse error: {e}")
        return {}


# ─── FastAPI app ──────────────────────────────────────────────────────────────
app = FastAPI(
    title="Welkin × Midea — amoCRM Webhook",
    description="Production webhook for all 5 bots",
    version="2.0"
)


@app.get("/")
def root():
    return {
        "service": "Welkin × Midea amoCRM Webhook",
        "version": "2.0",
        "endpoints": {
            "POST /webhook/lead":          "Bot #1 Consultant — create new lead",
            "POST /webhook/warm":          "Bot #2 Warmer — move to warm stage",
            "POST /webhook/reactivate":    "Bot #3 Reactivator — reactivate cold lead",
            "POST /webhook/service":       "Bot #4 Service — post-sale actions",
            "POST /webhook/referral":      "Bot #5 Referral — create referral lead",
            "POST /webhook/update_status": "Universal — update lead status",
            "GET  /health":                "Health check",
        }
    }


@app.get("/health")
def health():
    try:
        ok = test_token()
        if ok:
            return {"status": "ok", "amo_api": "connected", "domain": AMO_DOMAIN}
        return JSONResponse(status_code=503, content={"status": "error", "amo_api": "token_invalid"})
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "error", "detail": str(e)})


# ─── Bot #1: Consultant ───────────────────────────────────────────────────────
@app.post("/webhook/lead")
async def bot1_create_lead(request: Request):
    """Bot #1 — Consultant: Create new lead in amoCRM."""
    body = await parse_body(request)
    logger.info(f"[Bot#1] {body}")

    lead_name = body.get("lead_name") or body.get("name") or "Новый лид"
    phone     = body.get("lead_phone") or body.get("phone", "")
    source    = body.get("source", "Instagram")

    try:
        contact_id = find_or_create_contact(lead_name, phone)
        lead_id = create_lead(
            name=lead_name,
            contact_id=contact_id,
            custom_fields=build_custom_fields({**body, "source": source}),
            pipeline_id=PIPELINE_ID,
            status_id=STATUS_NEW_LEAD,
            tags=["бот", "консультант"]
        )
        note = build_note(body, "Консультант (@mideazubot)")
        add_note(lead_id, note)
        return {"success": True, "lead_id": lead_id, "contact_id": contact_id,
                "message": f"Lead '{lead_name}' created"}
    except requests.HTTPError as e:
        logger.error(f"[Bot#1] HTTP {e.response.status_code}: {e.response.text[:300]}")
        raise HTTPException(status_code=502, detail=f"amoCRM error: {e.response.status_code}")
    except Exception as e:
        logger.error(f"[Bot#1] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ─── Bot #2: Warmer ───────────────────────────────────────────────────────────
@app.post("/webhook/warm")
async def bot2_warm(request: Request):
    """Bot #2 — Warmer: Move lead to warm stage or create warm lead."""
    body = await parse_body(request)
    logger.info(f"[Bot#2] {body}")

    lead_name = body.get("lead_name") or body.get("name") or "Тёплый лид"
    phone     = body.get("lead_phone") or body.get("phone", "")
    lead_id   = body.get("lead_id")
    note      = build_note(body, "Прогревщик (@welkin_warm_bot)")

    try:
        if lead_id:
            update_lead_status(int(lead_id), STATUS_WARM)
            add_note(int(lead_id), f"🔥 Передан в прогрев\n{note}")
            add_tag(int(lead_id), "прогрев")
            return {"success": True, "lead_id": int(lead_id), "message": "Moved to warm stage"}

        contact_id = find_or_create_contact(lead_name, phone)
        existing = find_lead_by_contact(contact_id)
        if existing:
            update_lead_status(existing, STATUS_WARM)
            add_note(existing, f"🔥 Передан в прогрев\n{note}")
            add_tag(existing, "прогрев")
            return {"success": True, "lead_id": existing, "message": "Existing lead moved to warm"}

        new_lid = create_lead(lead_name, contact_id, build_custom_fields(body),
                              PIPELINE_ID, STATUS_WARM, ["бот", "прогрев"])
        add_note(new_lid, note)
        return {"success": True, "lead_id": new_lid, "message": "Warm lead created"}
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"amoCRM error: {e.response.status_code}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Bot #3: Reactivator ──────────────────────────────────────────────────────
@app.post("/webhook/reactivate")
async def bot3_reactivate(request: Request):
    """Bot #3 — Reactivator: Reactivate cold/silent leads."""
    body = await parse_body(request)
    logger.info(f"[Bot#3] {body}")

    lead_name = body.get("lead_name") or body.get("name") or "Реактивация"
    phone     = body.get("lead_phone") or body.get("phone", "")
    lead_id   = body.get("lead_id")
    note      = build_note(body, "Реактиватор (@welkin_reactivate_bot)")

    try:
        if lead_id:
            update_lead_status(int(lead_id), STATUS_REACTIVATE)
            add_note(int(lead_id), f"⚡ Реактивация!\n{note}")
            add_tag(int(lead_id), "реактивация")
            return {"success": True, "lead_id": int(lead_id), "message": "Lead reactivated"}

        contact_id = find_or_create_contact(lead_name, phone)
        existing = find_lead_by_contact(contact_id)
        if existing:
            update_lead_status(existing, STATUS_REACTIVATE)
            add_note(existing, f"⚡ Реактивация!\n{note}")
            add_tag(existing, "реактивация")
            return {"success": True, "lead_id": existing, "message": "Existing lead reactivated"}

        new_lid = create_lead(lead_name, contact_id, build_custom_fields(body),
                              PIPELINE_ID, STATUS_REACTIVATE, ["бот", "реактивация"])
        add_note(new_lid, note)
        return {"success": True, "lead_id": new_lid, "message": "Reactivation lead created"}
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"amoCRM error: {e.response.status_code}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Bot #4: Service ──────────────────────────────────────────────────────────
@app.post("/webhook/service")
async def bot4_service(request: Request):
    """Bot #4 — Service: Post-sale actions (NPS, cross-sell, maintenance)."""
    body = await parse_body(request)
    logger.info(f"[Bot#4] {body}")

    lead_name = body.get("lead_name") or body.get("name") or "Клиент"
    phone     = body.get("lead_phone") or body.get("phone", "")
    lead_id   = body.get("lead_id")
    action    = body.get("action", "note")
    nps       = body.get("nps_rating") or body.get("rating", "")

    try:
        if not lead_id:
            contact_id = find_or_create_contact(lead_name, phone)
            lead_id = find_lead_by_contact(contact_id)

        if lead_id:
            svc_note = f"🔧 Сервисный бот\n📋 Действие: {action}"
            extra = body.get("note") or body.get("comment", "")
            if extra:
                svc_note += f"\n{extra}"
            if nps:
                svc_note += f"\n⭐ NPS оценка: {nps}/5"
            add_note(int(lead_id), svc_note)
            add_tag(int(lead_id), f"сервис")
            return {"success": True, "lead_id": int(lead_id), "message": f"Service '{action}' recorded"}

        contact_id = find_or_create_contact(lead_name, phone)
        new_lid = create_lead(f"Сервис: {lead_name}", contact_id, [],
                              PIPELINE_ID, STATUS_WON, ["сервис"])
        add_note(new_lid, f"🔧 Сервисный бот\n{action}")
        return {"success": True, "lead_id": new_lid, "message": "Service lead created"}
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"amoCRM error: {e.response.status_code}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Bot #5: Referral ─────────────────────────────────────────────────────────
@app.post("/webhook/referral")
async def bot5_referral(request: Request):
    """Bot #5 — Referral: Create referral lead."""
    body = await parse_body(request)
    logger.info(f"[Bot#5] {body}")

    lead_name      = body.get("lead_name") or body.get("name") or "Реферальный лид"
    phone          = body.get("lead_phone") or body.get("phone", "")
    referrer_name  = body.get("referrer_name", "")
    referrer_phone = body.get("referrer_phone", "")
    referral_code  = body.get("referral_code", "")

    note_parts = ["🤝 Реферальный бот (@welkin_refer_bot)"]
    if referrer_name:  note_parts.append(f"👤 Привёл: {referrer_name}")
    if referrer_phone: note_parts.append(f"📞 Реферер: {referrer_phone}")
    if referral_code:  note_parts.append(f"🔑 Код: {referral_code}")
    note = "\n".join(note_parts)

    try:
        contact_id = find_or_create_contact(lead_name, phone)
        lead_id = create_lead(
            name=f"Реферал: {lead_name}",
            contact_id=contact_id,
            custom_fields=build_custom_fields({**body, "source": "реферал"}),
            pipeline_id=PIPELINE_ID,
            status_id=STATUS_NEW_LEAD,
            tags=["бот", "реферал"]
        )
        add_note(lead_id, note)
        return {"success": True, "lead_id": lead_id, "contact_id": contact_id,
                "message": f"Referral lead '{lead_name}' created"}
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"amoCRM error: {e.response.status_code}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Universal status update ──────────────────────────────────────────────────
@app.post("/webhook/update_status")
async def update_status(request: Request):
    """Universal: update lead status/stage."""
    body = await parse_body(request)
    lead_id   = body.get("lead_id")
    status_id = body.get("status_id")
    note      = body.get("note", "")
    if not lead_id or not status_id:
        raise HTTPException(status_code=400, detail="lead_id and status_id required")
    try:
        ok = update_lead_status(int(lead_id), int(status_id))
        if note:
            add_note(int(lead_id), note)
        return {"success": ok, "lead_id": int(lead_id), "new_status_id": int(status_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8765"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
