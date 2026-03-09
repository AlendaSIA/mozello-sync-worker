from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

@app.route("/process", methods=["POST"])
def process():
    data = request.get_json(force=True, silent=True) or {}
    print("mozello-sync-worker received:", data)
    return jsonify({"ok": True, "received": data})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
