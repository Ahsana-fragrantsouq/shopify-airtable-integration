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
SKU_TABLE = "tblI3DHGUT2GRINfw"   # French Inventories

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

# ---------------- SECURITY ----------------
def verify_webhook(data, hmac_header):
    if not hmac_header:
        return False

    digest = hmac.new(
        SHOPIFY_WEBHOOK_SECRET.encode("utf-8"),
        data,
        hashlib.sha256
    ).digest()

    computed_hmac = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(computed_hmac, hmac_header)


# ---------------- AIRTABLE HELPERS ----------------
def find_customer(phone, email):
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
        return data["records"][0]["id"]

    return None


def create_customer(customer):
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
    data = r.json()

    if data.get("records"):
        return data["records"][0]["id"]

    return None


# ---------------- DUPLICATE CHECK ----------------
def order_exists(order_id):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"
    r = requests.get(
        url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{Order ID}}='{order_id}'"}
    )
    return bool(r.json().get("records"))


# ---------------- SHIPPING STATUS UPDATE ----------------
def update_shipping_status(order_id, status):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"

    r = requests.get(
        url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{Order ID}}='{order_id}'"}
    )

    records = r.json().get("records", [])
    if not records:
        return

    record_id = records[0]["id"]

    update_url = f"{url}/{record_id}"
    payload = {
        "fields": {
            "Shipping Status": status
        }
    }

    requests.patch(update_url, headers=AIRTABLE_HEADERS, json=payload)
    print(f"🚚 Shipping Status updated → {status}", flush=True)


# ---------------- ORDER CREATION ----------------
def create_order(order, customer_id):
    print("🧾 Creating order record...", flush=True)

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

    payload = {"fields": fields}

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"
    r = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)

    print("📨 Order insert status:", r.status_code, flush=True)
    print("📨 Order insert body:", r.text, flush=True)


# ---------------- MAIN LOGIC (ORDERS) ----------------
def process_order(order):
    order_id = str(order["id"])

    # If already exists → do nothing here
    if order_exists(order_id):
        return

    customer = order.get("customer") or {}

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
        return

    create_order(order, customer_id)


# ---------------- WEBHOOK : ORDERS ----------------
@app.route("/shopify/webhook/orders", methods=["POST"])
def shopify_orders():
    data = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    if not verify_webhook(data, hmac_header):
        return "Unauthorized", 401

    process_order(request.json)
    return jsonify({"status": "ok"})


# ---------------- WEBHOOK : FULFILLMENTS ----------------
@app.route("/shopify/webhook/fulfillments", methods=["POST"])
def shopify_fulfillments():
    data = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    if not verify_webhook(data, hmac_header):
        return "Unauthorized", 401

    payload = request.json

    order_id = payload.get("order_id")
    if not order_id:
        return jsonify({"status": "no order id"}), 200

    update_shipping_status(str(order_id), "Shipped")
    return jsonify({"status": "shipped"})
