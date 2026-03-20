import os
import time
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
    "https://go.paytraq.com/ext/webhooks/inbox/986931409703967629"
)

STEP0_TRIGGER_URL = os.getenv(
    "STEP0_TRIGGER_URL",
    "https://step0-trigger-142693968214.europe-west1.run.app/run"
)
STEP0_TRIGGER_AUDIENCE = os.getenv(
    "STEP0_TRIGGER_AUDIENCE",
    "https://step0-trigger-142693968214.europe-west1.run.app"
)

ORDER_CREATED_WAIT_BEFORE_TRIGGER_SEC = int(
    os.getenv("ORDER_CREATED_WAIT_BEFORE_TRIGGER_SEC", "90")
)
PAYMENT_CHANGED_WAIT_SEC = int(
    os.getenv("PAYMENT_CHANGED_WAIT_SEC", "180")
)

# Pipedrive fields
PD_FIELD_PAYMENT_METHOD = "481737790ef7c52078ce5455742c0a0ad32a0f8e"
PD_FIELD_PAYMENT_STATUS = "990cd54bfc733795ffc5888156053301245261b7"
PD_FIELD_SHIPPING_METHOD = "7f74e6eca96c93d5ecc3cda935f6cf3ead9a60fb"
PD_FIELD_TRACKING_URL = "fa93f8d95879ea0c0a92f99d9a84fe125e79d3ff"
PD_FIELD_TRACKING_CODE = "2422006d4620be8a343499abdece3bc9fe6f5b14"
PD_FIELD_DISPATCH_DATE = "e98a18db6058dca682dd00f3161f665b4b739e88"

EVENT_ORDER_CREATED = "ORDER_CREATED"
EVENT_PAYMENT_CHANGED = "PAYMENT_CHANGED"

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
            return v.upper()

    if isinstance(body.get("data"), dict):
        for k in ("event", "type", "notification_type"):
            v = _pt(body["data"].get(k))
            if v:
                return v.upper()

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
        "text": (r.text or "")[:500],
    }


def get_identity_token(audience: str) -> str:
    metadata_url = (
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity"
    )
    r = requests.get(
        metadata_url,
        params={"audience": audience, "format": "full"},
        headers={"Metadata-Flavor": "Google"},
        timeout=20,
    )
    r.raise_for_status()
    return r.text.strip()


def call_step0_trigger() -> Dict[str, Any]:
    token = get_identity_token(STEP0_TRIGGER_AUDIENCE)
    r = requests.post(
        STEP0_TRIGGER_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={},
        timeout=120,
    )

    preview = None
    try:
        preview = r.json()
    except Exception:
        preview = {"raw": (r.text or "")[:500]}

    return {
        "ok": r.ok,
        "http_status": r.status_code,
        "response": preview,
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
    doc_ref = extract_doc_ref(body)

    print("mozello-sync: event_type=", event_type)
    print("mozello-sync: doc_ref=", doc_ref)

    if not doc_ref:
        print("mozello-sync: missing_doc_ref")
        return {
            "ok": True,
            "skipped": True,
            "reason": "missing_doc_ref",
            "event_type": event_type,
        }, 200

    if event_type == EVENT_ORDER_CREATED:
        print("mozello-sync: ORDER_CREATED -> forward to PayTraq")
        try:
            paytraq_forward = forward_to_paytraq_raw(body)
            print("mozello-sync: paytraq_forward=", paytraq_forward)
        except Exception as e:
            print("mozello-sync: paytraq_forward_error=", str(e))
            return {
                "ok": False,
                "event_type": event_type,
                "doc_ref": doc_ref,
                "reason": f"paytraq_forward_failed: {e}",
            }, 500

        print(
            "mozello-sync: ORDER_CREATED waiting",
            ORDER_CREATED_WAIT_BEFORE_TRIGGER_SEC,
            "sec before step0-trigger",
        )
        time.sleep(ORDER_CREATED_WAIT_BEFORE_TRIGGER_SEC)

        try:
            trigger_result = call_step0_trigger()
            print("mozello-sync: step0_trigger_result=", trigger_result)
        except Exception as e:
            print("mozello-sync: step0_trigger_error=", str(e))
            return {
                "ok": False,
                "event_type": event_type,
                "doc_ref": doc_ref,
                "paytraq_forward": paytraq_forward,
                "reason": f"step0_trigger_failed: {e}",
            }, 500

        return {
            "ok": True,
            "event_type": event_type,
            "doc_ref": doc_ref,
            "paytraq_forward": paytraq_forward,
            "step0_trigger": trigger_result,
            "mode": "order_created_forward_then_trigger_only",
        }, 200

    if event_type == EVENT_PAYMENT_CHANGED:
        print(
            "mozello-sync: PAYMENT_CHANGED waiting",
            PAYMENT_CHANGED_WAIT_SEC,
            "sec before DB/deal update",
        )
        time.sleep(PAYMENT_CHANGED_WAIT_SEC)

        mapping = fetch_mapping(doc_ref)
        if not mapping:
            print("mozello-sync: mapping_not_found doc_ref=", doc_ref)
            return {
                "ok": True,
                "skipped": True,
                "reason": "mapping_not_found",
                "doc_ref": doc_ref,
                "event_type": event_type,
            }, 200

        deal_id = mapping["pipedrive_deal_id"]
        print("mozello-sync: mapping_found doc_ref=", doc_ref, "deal_id=", deal_id)

        try:
            order = fetch_mozello_order(doc_ref)
            print(
                "mozello-sync: mozello_order payment_status=",
                _pt(order.get("payment_status")),
                "payment_method=",
                _pt(order.get("payment_method_details")) or _pt(order.get("payment_method")),
            )

            deal = fetch_pipedrive_deal(deal_id)
            payload = build_payload(order, deal)
            print("mozello-sync: pipedrive_payload=", payload)

            result = update_deal(deal_id, payload)
            print("mozello-sync: pipedrive_update_done deal_id=", deal_id)
        except Exception as e:
            print("mozello-sync: payment_changed_processing_failed=", str(e))
            return {
                "ok": True,
                "skipped": True,
                "reason": f"payment_changed_processing_failed: {e}",
                "doc_ref": doc_ref,
                "deal_id": deal_id,
                "event_type": event_type,
            }, 200

        return {
            "ok": True,
            "event_type": event_type,
            "doc_ref": doc_ref,
            "deal_id": deal_id,
            "payload": payload,
            "pipedrive_response": result,
            "mode": "payment_changed_wait_then_update_deal",
        }, 200

    print("mozello-sync: event_ignored event_type=", event_type)
    return {
        "ok": True,
        "skipped": True,
        "reason": f"event_ignored:{event_type or 'unknown'}",
        "doc_ref": doc_ref,
        "event_type": event_type,
    }, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
