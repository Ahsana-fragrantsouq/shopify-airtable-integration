import os
import hmac
import hashlib
import base64
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

print("🚀 Flask Shopify → Airtable Service Started", flush=True)

# ---------------- ENV ----------------
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = "app2jovFGPe7hkYdB"
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

# ---------------- AIRTABLE HELPERS ----------------
def order_exists(order_id):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"
    r = requests.get(
        url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{Order ID}}='{order_id}'"}
    )
    exists = bool(r.json().get("records"))
    print(f"🔎 Order {order_id} exists in Airtable:", exists, flush=True)
    return exists


def find_customer(phone, email):
    if phone:
        formula = f"{{Contact Number}}='{phone}'"
    elif email:
        formula = f"{{Mail id}}='{email}'"
    else:
        return None

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{CUSTOMERS_TABLE}"
    r = requests.get(url, headers=AIRTABLE_HEADERS, params={"filterByFormula": formula})
    records = r.json().get("records", [])
    return records[0]["id"] if records else None


def create_customer(customer):
    print("➕ Creating customer:", customer["name"], flush=True)

    payload = {
        "fields": {
            "Name": customer["name"],
            "Mail id": customer.get("email"),
            "Contact Number": customer.get("phone"),
            "Address": customer.get("address"),
            "Acquired sales channel": "Shopify"
        }
    }

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{CUSTOMERS_TABLE}"
    r = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)
    return r.json().get("id")


def find_sku_record(sku):
    if not sku:
        return None

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{SKU_TABLE}"
    r = requests.get(
        url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{SKU}}='{sku}'"}
    )
    records = r.json().get("records", [])
    return records[0]["id"] if records else None


# ---------------- CREATE ORDER ----------------
def create_order(order, customer_id):
    print("🧾 Creating NEW order in Airtable", flush=True)

    order_date = order["created_at"].split("T")[0]

    sku_records = []
    for line in order.get("line_items", []):
        sku_id = find_sku_record(line.get("sku"))
        if sku_id:
            sku_records.append(sku_id)

    fields = {
        "Order ID": str(order["id"]),
        "Order Number": order.get("name", "").replace("#", ""),
        "Customer": [customer_id],
        "Order Date": order_date,
        "Total Order Amount": float(order["subtotal_price"]),
        "Payment Status": order["financial_status"].capitalize(),
        "Shipping Status": "New",
        "Sales Channel": "Online Store",
        "Order Packing Slip": [{"url": order.get("order_status_url")}]
    }

    if sku_records:
        fields["Item SKU"] = sku_records
        fields["Brands"] = sku_records

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"
    r = requests.post(url, headers=AIRTABLE_HEADERS, json={"fields": fields})

    print("📨 Airtable response:", r.status_code, r.text, flush=True)


# ---------------- MAIN ORDER LOGIC ----------------
def process_order(order):
    order_id = str(order["id"])
    print("📦 Shopify Order Received:", order_id, flush=True)

    # ✅ ONLY NEW ORDERS
    if order_exists(order_id):
        print("⏭️ Existing order ignored (as designed)", flush=True)
        return

    customer = order.get("customer") or {}

    customer_id = find_customer(customer.get("phone"), customer.get("email"))
    if not customer_id:
        customer_id = create_customer({
            "name": f"{customer.get('first_name','')} {customer.get('last_name','')}".strip() or "Unknown",
            "email": customer.get("email"),
            "phone": customer.get("phone"),
            "address": order.get("shipping_address", {}).get("address1")
        })

    if not customer_id:
        print("❌ Customer creation failed", flush=True)
        return

    create_order(order, customer_id)
    print("✅ New order saved to Airtable", flush=True)


# ---------------- WEBHOOK ----------------
@app.route("/shopify/webhook/orders", methods=["POST"])
def shopify_orders():
    data = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    if not verify_webhook(data, hmac_header):
        return "Unauthorized", 401

    process_order(request.json)
    return jsonify({"status": "ok"})
