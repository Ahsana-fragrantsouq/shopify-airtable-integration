import os
import hmac
import hashlib
import base64
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

print("🚀 Shopify → Airtable service started", flush=True)

# ---------------- ENV ----------------
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = "appJsqKCta3lkgdJJ"   # NEW BASE
SHOPIFY_WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET")

CUSTOMERS_TABLE = "tblas8rMuwMEAtjIv"
ORDERS_TABLE    = "tbl1bAQM8lBgsGrqh"
SKU_TABLE       = "tblI3DHGUT2GRINfw"

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

# ---------------- SECURITY ----------------
def verify_webhook(data, hmac_header):
    if not hmac_header:
        return False

    digest = hmac.new(
        SHOPIFY_WEBHOOK_SECRET.encode(),
        data,
        hashlib.sha256
    ).digest()

    computed = base64.b64encode(digest).decode()
    return hmac.compare_digest(computed, hmac_header)

# ---------------- HELPERS ----------------
def order_exists(order_id):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"
    r = requests.get(
        url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{Order ID}}='{order_id}'"}
    )
    return bool(r.json().get("records"))

def update_shipping_status(order_id, status):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"

    r = requests.get(
        url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{Order ID}}='{order_id}'"}
    )

    records = r.json().get("records", [])
    if not records:
        print("⚠️ Order not found in Airtable for shipping update", flush=True)
        return

    record_id = records[0]["id"]

    requests.patch(
        f"{url}/{record_id}",
        headers=AIRTABLE_HEADERS,
        json={"fields": {"Shipping Status": status}}
    )

    print(f"🚚 Shipping Status updated → {status}", flush=True)

# ---------------- ORDERS ----------------
@app.route("/shopify/webhook/orders", methods=["POST"])
def shopify_orders():
    data = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    if not verify_webhook(data, hmac_header):
        return "Unauthorized", 401

    order = request.json
    order_id = str(order["id"])

    # ONLY NEW ORDERS
    if order_exists(order_id):
        print("⏭️ Order already exists, skipped", flush=True)
        return jsonify({"status": "skipped"})

    print("🧾 Creating new order in Airtable", flush=True)

    order_date = order["created_at"].split("T")[0]

    fields = {
        "Order ID": order_id,
        "Order Number": order.get("name", "").replace("#", ""),
        "Order Date": order_date,
        "Shipping Status": "New",
        "Payment Status": order["financial_status"].capitalize(),
        "Sales Channel": "Online Store",
        "Order Packing Slip": [{"url": order.get("order_status_url")}]
    }

    requests.post(
        f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}",
        headers=AIRTABLE_HEADERS,
        json={"fields": fields}
    )

    print("✅ Order created", flush=True)
    return jsonify({"status": "created"})

# ---------------- FULFILLMENTS ----------------
@app.route("/shopify/webhook/fulfillments", methods=["POST"])
def shopify_fulfillments():
    data = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    if not verify_webhook(data, hmac_header):
        return "Unauthorized", 401

    payload = request.json
    order_id = payload.get("order_id")

    if not order_id:
        return jsonify({"status": "no order id"})

    update_shipping_status(str(order_id), "Shipped")
    return jsonify({"status": "shipped"})
