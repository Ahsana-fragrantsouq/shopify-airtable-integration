import os
import hmac
import hashlib
import base64
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

print("🚀 Flask Shopify Airtable Service Starting...", flush=True)

# ---------------- ENV ----------------
AIRTABLE_TOKEN         = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID       = "app5gOqDt9aZrW5bV"
SHOPIFY_WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET")

# Airtable TABLE IDs
CUSTOMERS_TABLE        = "tbldpymKhQIwK5qGP"   # Customers
ORDER_LINE_ITEMS_TABLE = "tblW0STW6AGKaFCOT"   # Order Line Items
SKU_TABLE              = "tblL03CEHdYy1kUdQ"     # French Inventories

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

# ---------------- SHOPIFY → AIRTABLE PAYMENT STATUS MAP ----------------
# Shopify financial_status  →  Airtable Payment Status select value
PAYMENT_STATUS_MAP = {
    "paid":             "Paid",
    "pending":          "Pending",
    "partially_paid":   "Pending",
    "refunded":         "Refunded",
    "voided":           "Voided",
    "partially_refunded": "Refunded",
    "authorized":       "Pending",
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
            "Name":                   customer["name"],
            "Mail id":                customer.get("email"),
            "Contact Number":         customer.get("phone"),
            "Address":                customer.get("address"),
            "Acquired sales channel": "Shopify"
        }
    }

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{CUSTOMERS_TABLE}"
    r = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)

    # ADD THESE TWO LINES
    print(f"👤 Customer create status: {r.status_code}", flush=True)
    print(f"👤 Customer create response: {r.text}", flush=True)

    return r.json().get("id")


def find_sku_record(sku):
    """
    Looks up a record in French Inventories by SKU field.
    Returns the Airtable record ID (used to link into 'Product' field).
    """
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
    """Returns True if any line item row already exists for this Shopify order ID."""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDER_LINE_ITEMS_TABLE}"
    r = requests.get(
        url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{Order ID}}='{order_id}'"}
    )
    return bool(r.json().get("records"))


# ---------------- SHIPPING STATUS UPDATE ----------------
def update_shipping_status(order_id, status):
    """Updates Shipping Status on ALL line item rows for this order."""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDER_LINE_ITEMS_TABLE}"

    r = requests.get(
        url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{Order ID}}='{order_id}'"}
    )

    records = r.json().get("records", [])
    if not records:
        print(f"⚠️ No line items found for Order ID {order_id}", flush=True)
        return

    for record in records:
        record_id  = record["id"]
        update_url = f"{url}/{record_id}"
        requests.patch(update_url, headers=AIRTABLE_HEADERS, json={
            "fields": {"Shipping Status": status}
        })

    print(f"🚚 Shipping Status → '{status}' on {len(records)} line item(s)", flush=True)


# ---------------- ORDER LINE ITEM CREATION ----------------
def create_order_line_items(order, customer_id):
    """
    Creates ONE Airtable record per Shopify line item in Order Line Items table.

    Field mapping:
      Product          ← linked record ID from French Inventories (matched by SKU)
      Order ID         ← order.id  (text, used for dedup & fulfillment lookup)
      Order Number     ← order.name stripped of '#'
      Customer         ← linked record ID from Customers table
      Order Date       ← order.created_at (date only)
      Rate             ← line_item.price  (price per unit)
      Qty              ← line_item.quantity
      Tax Type         ← tax rate as percentage string e.g. "5%"
      Sub Total        ← Rate × Qty
      Tax Amount       ← sum of tax_lines prices
      Amount           ← Sub Total + Tax Amount
      Payment Status   ← mapped from order.financial_status
      Shipping Status  ← "New"
      Sales Channel    ← "Shopify"
    """
    print("🧾 Creating order line item records...", flush=True)

    order_date     = order["created_at"].split("T")[0]
    order_id       = str(order["id"])
    order_number   = order.get("name", "").replace("#", "")
    shopify_status = order.get("financial_status", "pending").lower()
    payment_status = PAYMENT_STATUS_MAP.get(shopify_status, "Pending")

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDER_LINE_ITEMS_TABLE}"

    for line in order.get("line_items", []):
        sku       = line.get("sku")
        product_id = find_sku_record(sku)  # French Inventories record ID

        price     = float(line.get("price", 0))
        qty       = int(line.get("quantity", 1))
        sub_total = round(price * qty, 2)

        # Tax calculation from Shopify tax_lines
        tax_lines  = line.get("tax_lines", [])
        tax_rate   = tax_lines[0].get("rate", 0) if tax_lines else 0
        tax_pct    = f"{int(tax_rate * 100)}%"
        tax_amount = round(sum(float(t.get("price", 0)) for t in tax_lines), 2)
        amount     = round(sub_total + tax_amount, 2)

        fields = {
            "Order ID":        order_id,
            "Order Number":    order_number,
            "Customer":        [customer_id],
            "Order Date":      order_date,
            "Rate":            price,
            "Qty":             qty,
            "Tax Type":        tax_pct,
            "Sub Total":       sub_total,
            "Tax Amount":      tax_amount,
            "Amount":          amount,
            "Payment Status":  payment_status,
            "Shipping Status": "New",
            "Sales Channel":   "Shopify",
        }

        # Link to French Inventories via 'Product' field
        # SKU (from Product) and Product Name (from Product) are auto-populated lookups
        if product_id:
            fields["Product"] = [product_id]
        else:
            print(f"⚠️ SKU '{sku}' not found in French Inventories — Product field left empty", flush=True)

        r = requests.post(url, headers=AIRTABLE_HEADERS, json={"fields": fields})

        if r.status_code in (200, 201):
            print(f"✅ Created line item: '{line.get('title')}' (SKU: {sku})", flush=True)
        else:
            print(f"❌ Failed line item '{line.get('title')}': {r.status_code} — {r.text}", flush=True)


# ---------------- MAIN LOGIC ----------------
def process_order(order):
    order_id = str(order["id"])

    if order_exists(order_id):
        print(f"⏭️ Order {order_id} already exists — skipping", flush=True)
        return

    customer    = order.get("customer") or {}
    customer_id = find_customer(customer.get("phone"), customer.get("email"))

    if not customer_id:
        customer_id = create_customer({
            "name":    f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip(),
            "email":   customer.get("email"),
            "phone":   customer.get("phone"),
            "address": order.get("shipping_address", {}).get("address1")
        })

    if not customer_id:
        print("❌ Could not find or create customer — aborting order", flush=True)
        return

    create_order_line_items(order, customer_id)


# ---------------- WEBHOOK : ORDERS ----------------
@app.route("/shopify/webhook/orders", methods=["POST"])
def shopify_orders():
    data        = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    # TEMPORARY — comment out for testing, restore after
    # if not verify_webhook(data, hmac_header):
    #     return "Unauthorized", 401

    process_order(request.json)
    return jsonify({"status": "ok"})


# ---------------- WEBHOOK : FULFILLMENTS ----------------
@app.route("/shopify/webhook/fulfillments", methods=["POST"])
def shopify_fulfillments():
    data        = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    if not verify_webhook(data, hmac_header):
        return "Unauthorized", 401

    payload  = request.json
    order_id = payload.get("order_id")

    if not order_id:
        return jsonify({"status": "no order id"}), 200

    update_shipping_status(str(order_id), "Shipped")
    return jsonify({"status": "shipped"})
