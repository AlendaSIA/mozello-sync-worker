import os
from typing import Any, Dict, Optional

import requests
from flask import Flask, request, jsonify
from google.cloud import firestore

MOZELLO_API_KEY = os.getenv("MOZELLO_API_KEY")
MOZELLO_MAP_COLLECTION = os.getenv("MOZELLO_MAP_COLLECTION", "mozello_deal_map")

app = Flask(__name__)


def _pt(v: Any) -> str:
    return (str(v) if v is not None else "").strip()


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


@app.route("/process", methods=["POST"])
def process():
    body = request.get_json(force=True, silent=True) or {}
    doc_ref = _pt(body.get("doc_ref"))

    if not doc_ref:
        return jsonify({"ok": False, "error": "missing doc_ref"}), 400

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

    try:
        mozello_result = fetch_mozello_order(doc_ref)
    except Exception as e:
        return jsonify({
            "ok": False,
            "doc_ref": doc_ref,
            "mapping_result": mapping_result,
            "error": f"mozello_fetch_failed: {e}"
        }), 500

    print(
        "mozello-sync-worker: process "
        f"doc_ref={doc_ref} "
        f"mapping_found={mapping_result.get('found')} "
        f"mozello_http_status={mozello_result.get('http_status')}"
    )

    return jsonify({
        "ok": True,
        "doc_ref": doc_ref,
        "mapping_result": mapping_result,
        "mozello_result": mozello_result,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
