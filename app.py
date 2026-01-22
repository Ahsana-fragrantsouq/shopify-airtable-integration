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
AIRTABLE_BASE_ID = "app2jovFGPe7hkYdB"   # fixed base id
SHOPIFY_WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET")

# Airtable TABLE IDs
CUSTOMERS_TABLE = "tblas8rMuwMEAtjIv"
ORDERS_TABLE = "tbl1bAQM8lBgsGrqh"
SKU_TABLE = "tblI3DHGUT2GRINfw"

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

# ---------------- SECURITY ----------------
def verify_webhook(data, hmac_header):
    print("🔐 Verifying Shopify webhook signature...", flush=True)

    if not hmac_header:
        print("⚠️ No HMAC header received", flush=True)
        return False

    digest = hmac.new(
        SHOPIFY_WEBHOOK_SECRET.encode("utf-8"),
        data,
        hashlib.sha256
    ).digest()

    computed_hmac = base64.b64encode(digest).decode("utf-8")

    print("📌 Shopify HMAC :", hmac_header, flush=True)
    print("📌 Computed HMAC:", computed_hmac, flush=True)

    valid = hmac.compare_digest(computed_hmac, hmac_header)

    print("🔐 Webhook valid:", valid, flush=True)
    return valid


# ---------------- AIRTABLE HELPERS ----------------
def find_customer(phone, email):
    print("🔍 Searching customer...", flush=True)

    if phone:
        formula = f"{{Contact Number}}='{phone}'"
    elif email:
        formula = f"{{Mail id}}='{email}'"
    else:
        print("⚠️ No phone or email to search", flush=True)
        return None

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{CUSTOMERS_TABLE}"
    params = {"filterByFormula": formula}

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
            "Mail id": customer.get("email"),
            "Contact Number": customer.get("phone"),
            "Address": customer.get("address"),
            "Acquired sales channel": "Online Store"
        }
    }

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{CUSTOMERS_TABLE}"
    r = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)

    print("📨 Airtable status:", r.status_code, flush=True)
    print("📨 Airtable body:", r.text, flush=True)

    data = r.json()

    if "id" not in data:
        print("❌ Airtable customer creation failed", flush=True)
        return None

    cid = data["id"]
    print("✅ Customer created:", cid, flush=True)
    return cid


def find_sku_record(sku):
    print("🔍 Searching SKU:", sku, flush=True)

    if not sku:
        return None

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{SKU_TABLE}"
    params = {"filterByFormula": f"{{SKU}}='{sku}'"}

    r = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
    data = r.json()

    if data.get("records"):
        sid = data["records"][0]["id"]
        print("✅ SKU found:", sid, flush=True)
        return sid

    print("❌ SKU not found", flush=True)
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

    print("📨 Order insert status:", r.status_code, flush=True)
    print("📨 Order insert body:", r.text, flush=True)


# ---------------- MAIN LOGIC ----------------
def process_order(order):
    print("📦 Processing Shopify order:", order.get("id"), flush=True)

    customer = order.get("customer", {})

    phone = customer.get("phone")
    email = customer.get("email")
    name = customer.get("first_name", "") + " " + customer.get("last_name", "")
    address = order.get("shipping_address", {}).get("address1", "")

    customer_id = find_customer(phone, email)

    if not customer_id:
        customer_id = create_customer({
            "name": name.strip() or "Unknown",
            "email": email,
            "phone": phone,
            "address": address
        })

    if not customer_id:
        print("⛔ Cannot create order because customer creation failed", flush=True)
        return

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
