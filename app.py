import os
import hmac
import hashlib
import base64
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

print("🚀 Flask Shopify Airtable Service Starting...", flush=True)

# ---------------- ENV ----------------
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
SHOPIFY_WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET")

ORDERS_TABLE = "Orders"
CUSTOMERS_TABLE = "Customers"
SKU_TABLE = "French Inventories"

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

# ---------------- SECURITY ----------------
def verify_webhook(data, hmac_header):
    print("🔐 Verifying Shopify webhook signature...", flush=True)

    digest = hmac.new(
        SHOPIFY_WEBHOOK_SECRET.encode(),
        data,
        hashlib.sha256
    ).digest()

    computed_hmac = base64.b64encode(digest).decode()

    valid = hmac.compare_digest(computed_hmac, hmac_header)

    print("🔐 Webhook valid:", valid, flush=True)
    return valid

# ---------------- AIRTABLE HELPERS ----------------
def find_customer_by_phone(phone):
    print("🔍 Searching customer by phone:", phone, flush=True)

    if not phone:
        print("⚠️ No phone provided", flush=True)
        return None

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{CUSTOMERS_TABLE}"
    params = {"filterByFormula": f"{{Contact Number}}='{phone}'"}

    r = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
    data = r.json()

    if data.get("records"):
        cid = data["records"][0]["id"]
        print("✅ Customer found:", cid, flush=True)
        return cid

    print("❌ Customer not found", flush=True)
    return None


def create_customer(customer):
    print("➕ Creating new customer:", customer["name"], flush=True)

    payload = {
        "fields": {
            "Name": customer["name"],
            "Mail id": customer["email"],
            "Contact Number": customer["phone"],
            "Address": customer["address"],
            "Acquired sales channel": "Online Store"
        }
    }

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{CUSTOMERS_TABLE}"
    r = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)

    cid = r.json()["id"]
    print("✅ Customer created:", cid, flush=True)
    return cid


def find_sku_record(sku):
    print("🔍 Searching SKU:", sku, flush=True)

    if not sku:
        print("⚠️ SKU empty", flush=True)
        return None

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{SKU_TABLE}"
    params = {"filterByFormula": f"{{SKU}}='{sku}'"}

    r = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
    data = r.json()

    if data.get("records"):
        sid = data["records"][0]["id"]
        print("✅ SKU found:", sid, flush=True)
        return sid

    print("❌ SKU not found in inventory", flush=True)
    return None


def create_order(order, customer_id):
    print("🧾 Creating order record...", flush=True)

    line = order["line_items"][0]
    sku_record = find_sku_record(line.get("sku"))

    fields = {
        "Order ID": str(order["id"]),
        "Customer": [customer_id],
        "Order Date": order["created_at"],
        "Total Order Amount": float(order["subtotal_price"]),
        "Payment Status": order["financial_status"].capitalize(),
        "Shipping Status": order["fulfillment_status"].capitalize() if order["fulfillment_status"] else "New",
        "Sales Channel": "Online Store",
        "Order Packing Slip": order.get("order_status_url")
    }

    if sku_record:
        fields["Item SKU"] = [sku_record]

    payload = {"fields": fields}

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"
    r = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)

    print("✅ Order inserted into Airtable:", r.status_code, flush=True)


# ---------------- MAIN LOGIC ----------------
def process_order(order):
    print("📦 Processing Shopify order:", order.get("id"), flush=True)

    customer = order["customer"]

    phone = customer.get("phone")
    email = customer.get("email")
    name = customer.get("first_name", "") + " " + customer.get("last_name", "")
    address = order.get("shipping_address", {}).get("address1", "")

    customer_id = find_customer_by_phone(phone)

    if not customer_id:
        customer_id = create_customer({
            "name": name,
            "email": email,
            "phone": phone,
            "address": address
        })

    create_order(order, customer_id)

    print("🎯 Order processing completed", flush=True)


# ---------------- WEBHOOK ----------------
@app.route("/shopify/webhook/orders", methods=["POST"])
def shopify_orders():
    print("🔔 Shopify webhook received", flush=True)

    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    data = request.get_data()

    if not verify_webhook(data, hmac_header):
        print("⛔ Webhook verification failed", flush=True)
        return "Unauthorized", 401

    order = request.json

    print("📥 Shopify Order JSON received", flush=True)

    process_order(order)

    return jsonify({"status": "ok"})
