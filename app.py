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
AIRTABLE_BASE_ID = "app2jovFGPe7hkYdB"
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
        return None

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{CUSTOMERS_TABLE}"
    r = requests.get(url, headers=AIRTABLE_HEADERS, params={"filterByFormula": formula})
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
            "Acquired sales channel": "Shopify"
        }
    }

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{CUSTOMERS_TABLE}"
    r = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)

    print("📨 Airtable status:", r.status_code, flush=True)
    print("📨 Airtable body:", r.text, flush=True)

    data = r.json()
    return data.get("id")


def find_sku_record(sku):
    if not sku:
        return None

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{SKU_TABLE}"
    r = requests.get(
        url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{SKU}}='{sku}'"}
    )
    data = r.json()

    if data.get("records"):
        sid = data["records"][0]["id"]
        print("✅ SKU found:", sid, flush=True)
        return sid

    print("❌ SKU not found:", sku, flush=True)
    return None


# ---------------- DUPLICATE CHECK ----------------
def order_exists(order_id):
    print("🔍 Checking if order already exists:", order_id, flush=True)

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"
    r = requests.get(
        url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{Order ID}}='{order_id}'"}
    )
    data = r.json()

    if data.get("records"):
        print("⚠️ Order already exists. Skipping insert.", flush=True)
        return True

    return False


# ---------------- ORDER CREATION ----------------
def create_order(order, customer_id):
    print("🧾 Creating order record...", flush=True)

    order_date = order["created_at"].split("T")[0]

    # collect ALL SKUs
    sku_records = []
    for line in order.get("line_items", []):
        sku_id = find_sku_record(line.get("sku"))
        if sku_id:
            sku_records.append(sku_id)

    fields = {
        "Order ID": str(order["id"]),
        "Customer": [customer_id],
        "Order Date": order_date,
        "Total Order Amount": float(order["subtotal_price"]),
        "Payment Status": order["financial_status"].capitalize(),
        "Shipping Status": (
            order["fulfillment_status"].capitalize()
            if order["fulfillment_status"] else "New"
        ),
        "Sales Channel": "Online Store",
        "Order Packing Slip": [
            {"url": order.get("order_status_url")}
        ]
    }

    if sku_records:
        fields["Item SKU"] = sku_records

    payload = {"fields": fields}

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"
    r = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)

    print("📨 Order insert status:", r.status_code, flush=True)
    print("📨 Order insert body:", r.text, flush=True)


# ---------------- MAIN LOGIC ----------------
def process_order(order):
    print("📦 Processing Shopify order:", order.get("id"), flush=True)

    customer = order.get("customer", {})

    customer_id = find_customer(
        customer.get("phone"),
        customer.get("email")
    )

    if not customer_id:
        customer_id = create_customer({
            "name": f"{customer.get('first_name','')} {customer.get('last_name','')}".strip(),
            "email": customer.get("email"),
            "phone": customer.get("phone"),
            "address": order.get("shipping_address", {}).get("address1")
        })

    if not customer_id:
        print("⛔ Customer creation failed", flush=True)
        return

    # 🚫 DUPLICATE ORDER PROTECTION
    if order_exists(str(order["id"])):
        return

    create_order(order, customer_id)
    print("🎯 Order processing completed", flush=True)


# ---------------- WEBHOOK ----------------
@app.route("/shopify/webhook/orders", methods=["POST"])
def shopify_orders():
    print("🔔 Shopify webhook received", flush=True)

    data = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    if not verify_webhook(data, hmac_header):
        return "Unauthorized", 401

    process_order(request.json)
    return jsonify({"status": "ok"})
