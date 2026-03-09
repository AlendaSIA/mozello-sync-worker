import os
from typing import Any, Dict
from datetime import datetime

import requests
from flask import Flask, request
from google.cloud import firestore

MOZELLO_API_KEY = os.getenv("MOZELLO_API_KEY")
MOZELLO_MAP_COLLECTION = os.getenv("MOZELLO_MAP_COLLECTION", "mozello_deal_map")

PIPEDRIVE_API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")
PIPEDRIVE_BASE_URL = os.getenv("PIPEDRIVE_BASE_URL", "https://api.pipedrive.com").rstrip("/")

PAYTRAQ_WEBHOOK_URL = os.getenv(
    "PAYTRAQ_WEBHOOK_URL",
    "https://go.paytraq.com/ext/webhooks/inbox/986931409704617518"
)

# Pipedrive fields
PD_FIELD_PAYMENT_METHOD = "481737790ef7c52078ce5455742c0a0ad32a0f8e"
PD_FIELD_PAYMENT_STATUS = "990cd54bfc733795ffc5888156053301245261b7"
PD_FIELD_SHIPPING_METHOD = "7f74e6eca96c93d5ecc3cda935f6cf3ead9a60fb"
PD_FIELD_TRACKING_URL = "fa93f8d95879ea0c0a92f99d9a84fe125e79d3ff"
PD_FIELD_TRACKING_CODE = "2422006d4620be8a343499abdece3bc9fe6f5b14"
PD_FIELD_DISPATCH_DATE = "e98a18db6058dca682dd00f3161f665b4b739e88"

app = Flask(__name__)


def _pt(v: Any) -> str:
    return (str(v) if v is not None else "").strip()


def _pd_params():
    return {"api_token": PIPEDRIVE_API_TOKEN}


def _pd_url(path: str):
    return f"{PIPEDRIVE_BASE_URL}{path}"


def _json_or_form_body():
    data = request.get_json(silent=True)
    if isinstance(data, dict):
        return data

    if request.form:
        return {k: request.form.get(k) for k in request.form.keys()}

    return {}


def extract_event_type(body: Dict[str, Any]) -> str:
    for k in ("event", "type", "notification_type"):
        v = _pt(body.get(k))
        if v:
            return v

    if isinstance(body.get("data"), dict):
        for k in ("event", "type", "notification_type"):
            v = _pt(body["data"].get(k))
            if v:
                return v

    return ""


def extract_doc_ref(body: Dict[str, Any]):
    for k in ("doc_ref", "invoice_id", "order_id"):
        v = _pt(body.get(k))
        if v:
            return v

    if isinstance(body.get("order"), dict):
        for k in ("order_id", "invoice_id", "doc_ref"):
            v = _pt(body["order"].get(k))
            if v:
                return v

    if isinstance(body.get("data"), dict):
        for k in ("order_id", "invoice_id", "doc_ref"):
            v = _pt(body["data"].get(k))
            if v:
                return v

    return ""


def forward_to_paytraq_raw(body: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(
        PAYTRAQ_WEBHOOK_URL,
        json=body,
        headers={"Content-Type": "application/json"},
        timeout=20,
    )
    return {
        "ok": r.ok,
        "http_status": r.status_code,
        "text": r.text[:500],
    }


def fetch_mozello_order(doc_ref: str):
    url = f"https://api.mozello.com/v1/store/order/{doc_ref}/"
    r = requests.get(
        url,
        headers={"Authorization": f"ApiKey {MOZELLO_API_KEY}"},
        timeout=20,
    )
    return r.json()


def fetch_mapping(doc_ref: str):
    db = firestore.Client()
    snap = db.collection(MOZELLO_MAP_COLLECTION).document(doc_ref).get()
    if not snap.exists:
        return None
    return snap.to_dict()


def fetch_pipedrive_deal(deal_id):
    r = requests.get(
        _pd_url(f"/v1/deals/{deal_id}"),
        params=_pd_params(),
        timeout=20,
    )
    return r.json()["data"]


def build_payload(order, deal):
    payload = {}

    payment_method = _pt(order.get("payment_method_details")) or _pt(order.get("payment_method"))
    payment_status = _pt(order.get("payment_status"))
    shipping_method = _pt(order.get("shipping_method"))
    tracking_url = _pt(order.get("shipping_tracking_url"))
    tracking_code = _pt(order.get("shipping_tracking_code"))

    if deal.get(PD_FIELD_PAYMENT_METHOD) != payment_method:
        payload[PD_FIELD_PAYMENT_METHOD] = payment_method

    if deal.get(PD_FIELD_PAYMENT_STATUS) != payment_status:
        payload[PD_FIELD_PAYMENT_STATUS] = payment_status

    if deal.get(PD_FIELD_SHIPPING_METHOD) != shipping_method:
        payload[PD_FIELD_SHIPPING_METHOD] = shipping_method

    if deal.get(PD_FIELD_TRACKING_URL) != tracking_url:
        payload[PD_FIELD_TRACKING_URL] = tracking_url

    if deal.get(PD_FIELD_TRACKING_CODE) != tracking_code:
        payload[PD_FIELD_TRACKING_CODE] = tracking_code

    if order.get("dispatched") is True:
        today = datetime.utcnow().date().isoformat()
        if deal.get(PD_FIELD_DISPATCH_DATE) != today:
            payload[PD_FIELD_DISPATCH_DATE] = today

    return payload


def update_deal(deal_id, payload):
    if not payload:
        return {"ok": True, "skipped": True}

    r = requests.put(
        _pd_url(f"/v1/deals/{deal_id}"),
        params=_pd_params(),
        json=payload,
        timeout=30,
    )
    return r.json()


@app.route("/health")
def health():
    return {"status": "ok"}


@app.route("/process", methods=["POST"])
def process():
    body = _json_or_form_body()
    event_type = extract_event_type(body)

    paytraq_forward = None
    try:
        paytraq_forward = forward_to_paytraq_raw(body)
    except Exception as e:
        paytraq_forward = {"ok": False, "error": str(e)}

    doc_ref = extract_doc_ref(body)
    if not doc_ref:
        return {
            "ok": True,
            "skipped": True,
            "reason": "missing_doc_ref",
            "event_type": event_type,
            "paytraq_forward": paytraq_forward,
        }, 200

    mapping = fetch_mapping(doc_ref)
    if not mapping:
        return {
            "ok": True,
            "skipped": True,
            "reason": "mapping_not_found",
            "doc_ref": doc_ref,
            "event_type": event_type,
            "paytraq_forward": paytraq_forward,
        }, 200

    deal_id = mapping["pipedrive_deal_id"]

    try:
        order = fetch_mozello_order(doc_ref)
        deal = fetch_pipedrive_deal(deal_id)
        payload = build_payload(order, deal)
        result = update_deal(deal_id, payload)
    except Exception as e:
        return {
            "ok": True,
            "skipped": True,
            "reason": f"post_forward_processing_failed: {e}",
            "doc_ref": doc_ref,
            "deal_id": deal_id,
            "event_type": event_type,
            "paytraq_forward": paytraq_forward,
        }, 200

    return {
        "ok": True,
        "event_type": event_type,
        "doc_ref": doc_ref,
        "deal_id": deal_id,
        "paytraq_forward": paytraq_forward,
        "payload": payload,
        "pipedrive_response": result,
    }, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
