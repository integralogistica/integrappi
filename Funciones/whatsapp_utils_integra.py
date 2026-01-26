# Funciones/whatsapp_utils_integra.py
import os
import httpx
from typing import Dict, Any, Optional

PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
WHATSAPP_API_TOKEN = os.getenv("WHATSAPP_API_TOKEN")

GRAPH_URL = f"https://graph.facebook.com/v22.0/{PHONE_NUMBER_ID}/messages"


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {WHATSAPP_API_TOKEN}",
        "Content-Type": "application/json",
    }


async def _post_graph(payload: Dict[str, Any], error_prefix: str = "WhatsApp"):
    if not WHATSAPP_API_TOKEN or not PHONE_NUMBER_ID:
        print("⚠️ Faltan WHATSAPP_API_TOKEN o WHATSAPP_PHONE_NUMBER_ID")
        return None

    async with httpx.AsyncClient() as client:
        resp = await client.post(GRAPH_URL, headers=_headers(), json=payload, timeout=20)

    if resp.status_code != 200:
        print(f"❌ {error_prefix}: {resp.status_code} - {resp.text}")
        return None

    return resp.json()


async def enviar_texto(to: str, texto: str):
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"preview_url": True, "body": texto},
    }
    return await _post_graph(payload, "Enviar texto")


async def enviar_template_con_parametros(
    to: str,
    template_name: str,
    language_code: str,
    body_params: list[str],
):
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": language_code},
            "components": [
                {
                    "type": "body",
                    "parameters": [{"type": "text", "text": p} for p in body_params],
                }
            ],
        },
    }
    return await _post_graph(payload, f"Template {template_name}")
