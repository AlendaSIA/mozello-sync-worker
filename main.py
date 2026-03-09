import os
import requests
from flask import Flask, request, jsonify

MOZELLO_API_KEY = os.getenv("MOZELLO_API_KEY")

app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


def fetch_mozello_order(doc_ref: str) -> dict:
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


@app.route("/process", methods=["POST"])
def process():
    body = request.get_json(force=True, silent=True) or {}
    doc_ref = body.get("doc_ref")

    if not doc_ref:
        return jsonify({"ok": False, "error": "missing doc_ref"}), 400

    mozello_result = fetch_mozello_order(doc_ref)

    return jsonify({
        "ok": True,
        "doc_ref": doc_ref,
        "mozello_result": mozello_result
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
