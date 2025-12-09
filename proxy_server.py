import os
import requests
from flask import Flask, request, jsonify, abort

app = Flask(__name__)

DBT_ACCOUNT_ID = os.environ.get("DBT_ACCOUNT_ID")
DBT_API_KEY = os.environ.get("DBT_API_KEY")    # Service token (dbtc_...)
PROXY_SECRET = os.environ.get("PROXY_SECRET")  # a random secret string for proxy auth

DBT_BASE = "https://mn615.us1.dbt.com/api/v2"


def require_secret():
    header = request.headers.get("X-Proxy-Secret") or request.args.get("proxy_secret")
    if not header or PROXY_SECRET is None or header != PROXY_SECRET:
        abort(401, description="Unauthorized")

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/latest_run", methods=["GET"])
def latest_run():
    require_secret()
    project_id = request.args.get("project_id")
    job_id = request.args.get("job_id")
    if not project_id or not job_id:
        return jsonify({"error": "project_id and job_id are required"}), 400

    url = f"{DBT_BASE}/accounts/{DBT_ACCOUNT_ID}/runs/"
    params = {
        "project_id": project_id,
        "job_definition_id": job_id,
        "order_by": "-id",
        "limit": 1
    }
    headers = {"Authorization": f"Token {DBT_API_KEY}"}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=15)
        r.raise_for_status()
        return jsonify(r.json()), 200
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

@app.route("/runs/<run_id>/artifacts/<artifact_name>", methods=["GET"])
def get_artifact(run_id, artifact_name):
    require_secret()
    # artifact name like manifest.json or run_results.json (send without .json or with)
    name = artifact_name
    if not name.endswith(".json"):
        name = f"{name}.json"
    url = f"{DBT_BASE}/accounts/{DBT_ACCOUNT_ID}/runs/{run_id}/artifacts/{name}"
    headers = {"Authorization": f"Token {DBT_API_KEY}"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        # return raw JSON from dbt cloud
        return jsonify(r.json()), 200
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
