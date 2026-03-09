import os
from typing import Any, Dict, Optional

import requests
from flask import Flask, request, jsonify
from google.cloud import firestore

MOZELLO_API_KEY = os.getenv("MOZELLO_API_KEY")
MOZELLO_MAP_COLLECTION = os.getenv("MOZELLO_MAP_COLLECTION", "mozello_deal_map")

PIPEDRIVE_API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")
PIPEDRIVE_BASE_URL = os.getenv("PIPEDRIVE_BASE_URL", "https://api.pipedrive.com").rstrip("/")

# Deal field keys
PD_FIELD_MOZELLO_PAYMENT_METHOD = "481737790ef7c52078ce5455742c0a0ad32a0f8e"
PD_FIELD_MOZELLO_PAYMENT_STATUS = "990cd54bfc733795ffc5888156053301245261b7"

app = Flask(__name__)


def _pt(v: Any) -> str:
    return (str(v) if v is not None else "").strip()


def _json_or_form_body() -> Dict[str, Any]:
    data = request.get_json(silent=True)
    if isinstance(data, dict):
        return data

    if request.form:
        return {k: request.form.get(k) for k in request.form.keys()}

    return {}


def _extract_doc_ref(body: Dict[str, Any]) -> str:
    # 1) direct internal call
    doc_ref = _pt(body.get("doc_ref"))
    if doc_ref:
        return doc_ref

    # 2) Mozello payment trigger docs use invoice_id in POST
    #    invoice_id is like M-860325-30568
    invoice_id = _pt(body.get("invoice_id"))
    if invoice_id:
        return invoice_id

    # fallback names just in case
    for key in ("order_id", "document_ref", "docId", "invoiceId"):
        v = _pt(body.get(key))
        if v:
            return v

    return ""


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


def fetch_mozello_order(doc_ref: str) -> Dict[str, Any]:
    url = f"https://api.mozello.com/v1/store/order/{doc_ref}/"
    headers = {"Authorization": f"ApiKey {MOZELLO_API_KEY}"}

    r = requests.get(url, headers=headers, timeout=20)

    try:
        data = r.json()
    except Exception:
        data = {"raw_text": r.text}

    return {
        "http_status": r.status_code,
        "data": data,
    }


def fetch_mozello_mapping(doc_ref: str) -> Dict[str, Any]:
    db = firestore.Client()
    snap = db.collection(MOZELLO_MAP_COLLECTION).document(doc_ref).get()

    if not snap.exists:
        return {
            "found": False,
            "doc_ref": doc_ref,
            "collection": MOZELLO_MAP_COLLECTION,
            "data": None,
        }

    data = snap.to_dict() or {}
    return {
        "found": True,
        "doc_ref": doc_ref,
        "collection": MOZELLO_MAP_COLLECTION,
        "data": data,
    }


def _pd_params() -> Dict[str, str]:
    if not PIPEDRIVE_API_TOKEN:
        raise RuntimeError("missing PIPEDRIVE_API_TOKEN")
    return {"api_token": PIPEDRIVE_API_TOKEN}


def _pd_url(path: str) -> str:
    return f"{PIPEDRIVE_BASE_URL}{path}"


def build_payment_update_payload(mozello_order: Dict[str, Any]) -> Dict[str, Any]:
    data = mozello_order.get("data") or {}

    payment_method = _pt(data.get("payment_method_details")) or _pt(data.get("payment_method"))
    payment_status = _pt(data.get("payment_status"))

    payload: Dict[str, Any] = {}

    if payment_method:
        payload[PD_FIELD_MOZELLO_PAYMENT_METHOD] = payment_method

    if payment_status:
        payload[PD_FIELD_MOZELLO_PAYMENT_STATUS] = payment_status

    return payload


def update_pipedrive_deal(deal_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not payload:
        return {
            "ok": True,
            "skipped": True,
            "reason": "empty_payload",
            "deal_id": deal_id,
            "payload": {},
        }

    url = _pd_url(f"/v1/deals/{deal_id}")
    r = requests.put(url, params=_pd_params(), json=payload, timeout=30)

    try:
        data = r.json()
    except Exception:
        data = {"raw_text": r.text}

    return {
        "ok": r.ok,
        "http_status": r.status_code,
        "deal_id": deal_id,
        "payload": payload,
        "response": data,
    }


@app.route("/process", methods=["POST"])
def process():
    body = _json_or_form_body()
    doc_ref = _extract_doc_ref(body)

    if not doc_ref:
        return jsonify({
            "ok": False,
            "error": "missing doc_ref_or_invoice_id",
            "received_keys": sorted(list(body.keys()))
        }), 400

    if not MOZELLO_API_KEY:
        return jsonify({"ok": False, "error": "missing MOZELLO_API_KEY"}), 500

    try:
        mapping_result = fetch_mozello_mapping(doc_ref)
    except Exception as e:
        return jsonify({
            "ok": False,
            "doc_ref": doc_ref,
            "error": f"firestore_mapping_read_failed: {e}"
        }), 500

    if not mapping_result.get("found"):
        return jsonify({
            "ok": False,
            "doc_ref": doc_ref,
            "mapping_result": mapping_result,
            "error": "mapping_not_found"
        }), 404

    deal_id_raw = (mapping_result.get("data") or {}).get("pipedrive_deal_id")
    try:
        deal_id = int(deal_id_raw)
    except Exception:
        return jsonify({
            "ok": False,
            "doc_ref": doc_ref,
            "mapping_result": mapping_result,
            "error": f"invalid_or_missing_pipedrive_deal_id: {deal_id_raw}"
        }), 500

    try:
        mozello_result = fetch_mozello_order(doc_ref)
    except Exception as e:
        return jsonify({
            "ok": False,
            "doc_ref": doc_ref,
            "mapping_result": mapping_result,
            "error": f"mozello_fetch_failed: {e}"
        }), 500

    if mozello_result.get("http_status") != 200:
        return jsonify({
            "ok": False,
            "doc_ref": doc_ref,
            "mapping_result": mapping_result,
            "mozello_result": mozello_result,
            "error": "mozello_order_fetch_non_200"
        }), 502

    try:
        pd_payload = build_payment_update_payload(mozello_result)
        pipedrive_update = update_pipedrive_deal(deal_id, pd_payload)
    except Exception as e:
        return jsonify({
            "ok": False,
            "doc_ref": doc_ref,
            "deal_id": deal_id,
            "mapping_result": mapping_result,
            "mozello_result": mozello_result,
            "error": f"pipedrive_update_failed: {e}"
        }), 500

    print(
        "mozello-sync-worker: process "
        f"doc_ref={doc_ref} "
        f"deal_id={deal_id} "
        f"mapping_found={mapping_result.get('found')} "
        f"mozello_http_status={mozello_result.get('http_status')} "
        f"pd_update_ok={pipedrive_update.get('ok')}"
    )

    status_code = 200 if pipedrive_update.get("ok") else 502

    return jsonify({
        "ok": bool(pipedrive_update.get("ok")),
        "doc_ref": doc_ref,
        "deal_id": deal_id,
        "received_keys": sorted(list(body.keys())),
        "mapping_result": mapping_result,
        "mozello_result": mozello_result,
        "pipedrive_update": pipedrive_update,
    }), status_code


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
