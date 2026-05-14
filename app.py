import os
import hmac
import hashlib
import base64
import threading
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

print("🚀 Flask Shopify Airtable Service Starting...", flush=True)

# ---------------- ENV ----------------
AIRTABLE_TOKEN         = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID       = "app5gOqDt9aZrW5bV"
SHOPIFY_WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET")
SHOPIFY_STORE          = os.getenv("SHOPIFY_STORE")   # e.g. fragrantsouq
SHOPIFY_TOKEN          = os.getenv("SHOPIFY_TOKEN")   # Admin API access token

# Airtable TABLE IDs
CUSTOMERS_TABLE        = "tbldpymKhQIwK5qGP"   # Customers
ORDERS_TABLE           = "tbl480LKVFx8CiyoB"   # Orders
ORDER_LINE_ITEMS_TABLE = "tblW0STW6AGKaFCOT"   # Order Line Items
SKU_TABLE              = "tblL03CEHdYy1kUdQ"   # French Inventories

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

# ---------------- BACKGROUND SYNC STATE ----------------
_sync_running = False
_sync_lock    = threading.Lock()

# ---------------- SHOPIFY → AIRTABLE PAYMENT STATUS MAP ----------------
PAYMENT_STATUS_MAP = {
    "paid":               "Paid",
    "pending":            "Pending",
    "partially_paid":     "Pending",
    "refunded":           "Refunded",
    "voided":             "Voided",
    "partially_refunded": "Refunded",
    "authorized":         "Pending",
}

# ---------------- SHOPIFY → AIRTABLE SHIPPING STATUS LOGIC ----------------
SHIPPED_STATUSES = {
    "label_printed",
    "label_purchased",
    "attempted_delivery",
    "ready_for_pickup",
    "confirmed",
    "in_transit",
    "out_for_delivery",
}

def determine_shipping_status_from_order(order):
    """
    Determine Airtable Shipping Status from a Shopify order's current state.
    Walks through any existing fulfillments and returns the most advanced state.
    """
    fulfillments = order.get("fulfillments") or []
    fulfillment_status = (order.get("fulfillment_status") or "").lower()

    has_delivered = False
    has_shipped   = False
    has_fulfilled = False

    for f in fulfillments:
        shipment_status = (f.get("shipment_status") or "").lower()
        f_status        = (f.get("status") or "").lower()

        if shipment_status == "delivered":
            has_delivered = True
        elif shipment_status in SHIPPED_STATUSES:
            has_shipped = True
        elif f_status == "success":
            has_fulfilled = True

    if has_delivered:
        return "Delivered"
    if has_shipped:
        return "Shipped"
    if has_fulfilled or fulfillment_status in ("fulfilled", "partial"):
        return "Fulfilled"
    return "New"


def determine_shipping_status_from_fulfillment(fulfillment):
    """
    Determine Airtable Shipping Status from a Shopify fulfillment webhook payload.
    """
    shipment_status = (fulfillment.get("shipment_status") or "").lower()
    f_status        = (fulfillment.get("status") or "").lower()

    if shipment_status == "delivered":
        return "Delivered"
    if shipment_status in SHIPPED_STATUSES:
        return "Shipped"
    if f_status == "success":
        return "Fulfilled"
    return "Fulfilled"


# ---------------- SECURITY ----------------
def verify_webhook(data, hmac_header):
    if not hmac_header or not SHOPIFY_WEBHOOK_SECRET:
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
            "Customer Name":          customer["name"],
            "Mail id":                customer.get("email"),
            "Contact Number":         customer.get("phone"),
            "Address":                customer.get("address"),
            "Acquired sales channel": "Shopify"
        }
    }
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{CUSTOMERS_TABLE}"
    r = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)
    print(f"👤 Customer create status: {r.status_code}", flush=True)
    print(f"👤 Customer create response: {r.text}", flush=True)
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
    records = r.json().get("records", [])
    if records:
        return records[0]["id"]
    return None


# ---------------- UPDATE EXISTING ORDER STATUSES ----------------
def refresh_existing_order_statuses(order):
    """
    Refresh Shipping Status + Payment Status on records that already exist
    in Airtable, based on current Shopify state.
    Updates both Orders table and Order Line Items table.
    """
    order_id     = str(order["id"])
    order_number = order.get("name", "?")

    shipping_status = determine_shipping_status_from_order(order)
    shopify_payment = (order.get("financial_status") or "pending").lower()
    payment_status  = PAYMENT_STATUS_MAP.get(shopify_payment, "Pending")

    # --- Update Orders table ---
    orders_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"
    r = requests.get(
        orders_url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{Order ID}}='{order_id}'"}
    )
    for record in r.json().get("records", []):
        requests.patch(
            f"{orders_url}/{record['id']}",
            headers=AIRTABLE_HEADERS,
            json={"fields": {
                "Shipping Status": shipping_status,
                "Payment Status":  payment_status,
            }}
        )

    # --- Update Order Line Items table ---
    line_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDER_LINE_ITEMS_TABLE}"
    r = requests.get(
        line_url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{Order ID}}='{order_id}'"}
    )
    line_records = r.json().get("records", [])
    for record in line_records:
        requests.patch(
            f"{line_url}/{record['id']}",
            headers=AIRTABLE_HEADERS,
            json={"fields": {
                "Shipping Status": shipping_status,
                "Payment Status":  payment_status,
            }}
        )

    print(
        f"🔄 {order_number} refreshed → Shipping: {shipping_status}, "
        f"Payment: {payment_status} ({len(line_records)} line item(s))",
        flush=True
    )


# ---------------- ORDERS TABLE ----------------
def create_order_record(order, customer_id):
    order_date      = order["created_at"].split("T")[0]
    order_id        = str(order["id"])
    order_number    = order.get("name", "").replace("#", "")
    shopify_payment = (order.get("financial_status") or "pending").lower()
    payment_status  = PAYMENT_STATUS_MAP.get(shopify_payment, "Pending")

    fields = {
        "Order ID":        order_id,
        "Customer":        [customer_id],
        "Order Date":      order_date,
        "Sales Channel":   "Shopify",
        "Shipping Status": determine_shipping_status_from_order(order),
        "Payment Status":  payment_status,
    }

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"
    r = requests.post(url, headers=AIRTABLE_HEADERS, json={"fields": fields})

    if r.status_code in (200, 201):
        order_record_id = r.json().get("id")
        print(f"✅ Created Orders record: {order_number} (Airtable ID: {order_record_id})", flush=True)
        return order_record_id
    else:
        print(f"❌ Failed to create Orders record: {r.status_code} — {r.text}", flush=True)
        return None


# ---------------- SHIPPING STATUS UPDATE ----------------
def update_shipping_status(order_id, status):
    # --- Update Orders table ---
    orders_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDERS_TABLE}"
    r = requests.get(
        orders_url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{Order ID}}='{order_id}'"}
    )
    order_records = r.json().get("records", [])
    for record in order_records:
        requests.patch(
            f"{orders_url}/{record['id']}",
            headers=AIRTABLE_HEADERS,
            json={"fields": {"Shipping Status": status}}
        )
    print(f"🚚 Orders table Shipping Status → '{status}'", flush=True)

    # --- Update Order Line Items table ---
    line_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDER_LINE_ITEMS_TABLE}"
    r = requests.get(
        line_url,
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": f"{{Order ID}}='{order_id}'"}
    )
    line_records = r.json().get("records", [])
    if not line_records:
        print(f"⚠️ No line items found for Order ID {order_id}", flush=True)
    for record in line_records:
        requests.patch(
            f"{line_url}/{record['id']}",
            headers=AIRTABLE_HEADERS,
            json={"fields": {"Shipping Status": status}}
        )
    print(f"🚚 Order Line Items Shipping Status → '{status}' on {len(line_records)} row(s)", flush=True)


# ---------------- ORDER LINE ITEM CREATION ----------------
def create_order_line_items(order, customer_id, order_record_id):
    print("🧾 Creating order line item records...", flush=True)

    order_date     = order["created_at"].split("T")[0]
    order_id       = str(order["id"])
    order_number   = order.get("name", "").replace("#", "")
    shopify_status = order.get("financial_status", "pending").lower()
    payment_status = PAYMENT_STATUS_MAP.get(shopify_status, "Pending")
    shipping_status = determine_shipping_status_from_order(order)

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ORDER_LINE_ITEMS_TABLE}"

    for line in order.get("line_items", []):
        sku        = line.get("sku")
        product_id = find_sku_record(sku)

        price     = float(line.get("price", 0))
        qty       = int(line.get("quantity", 1))

        tax_lines = line.get("tax_lines", [])
        tax_rate  = tax_lines[0].get("rate", 0) if tax_lines else 0
        tax_pct   = f"{int(tax_rate * 100)}%"

        fields = {
            "Order ID":        order_id,
            "Order Number":    order_number,
            "Customer":        [customer_id],
            "Order Date":      order_date,
            "Rate":            price,
            "Qty":             qty,
            "Tax Type":        "5%",
            "Payment Status":  payment_status,
            "Shipping Status": shipping_status,
            "Sales Channel":   "Shopify",
        }

        if order_record_id:
            fields["Order"] = [order_record_id]

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

    existing_order_record_id = order_exists(order_id)
    if existing_order_record_id:
        print(f"⏭️ Order {order_id} already exists in Orders table — skipping", flush=True)
        return

    customer    = order.get("customer") or {}
    customer_id = find_customer(customer.get("phone"), customer.get("email"))

    if not customer_id:
        customer_id = create_customer({
            "name":    f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip(),
            "email":   customer.get("email"),
            "phone":   customer.get("phone"),
            "address": (order.get("shipping_address") or {}).get("address1")
        })

    if not customer_id:
        print("❌ Could not find or create customer — aborting order", flush=True)
        return

    order_record_id = create_order_record(order, customer_id)
    if not order_record_id:
        print("❌ Could not create Orders record — aborting line items", flush=True)
        return

    create_order_line_items(order, customer_id, order_record_id)


# ---------------- WEBHOOK : ORDERS ----------------
@app.route("/shopify/webhook/orders", methods=["POST"])
def shopify_orders():
    data        = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    if not verify_webhook(data, hmac_header):
        return "Unauthorized", 401

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

    new_status = determine_shipping_status_from_fulfillment(payload)
    update_shipping_status(str(order_id), new_status)
    return jsonify({"status": new_status.lower()})


# ---------------- SYNC ALL SHOPIFY ORDERS ----------------
def fetch_all_shopify_orders():
    orders = []
    url    = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/orders.json"
    params = {"limit": 250, "status": "any"}

    while url:
        r     = requests.get(url, headers={"X-Shopify-Access-Token": SHOPIFY_TOKEN}, params=params)
        batch = r.json().get("orders", [])
        orders.extend(batch)
        print(f"📦 Fetched {len(batch)} orders (total: {len(orders)})", flush=True)

        link   = r.headers.get("Link", "")
        url    = None
        params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
                    break

    return orders


def _do_full_sync():
    """Run the complete Shopify → Airtable sync in a background thread."""
    global _sync_running

    try:
        all_orders = fetch_all_shopify_orders()
        print(f"✅ Total orders from Shopify: {len(all_orders)}", flush=True)

        synced  = 0
        updated = 0
        failed  = 0

        for order in all_orders:
            order_name = order.get("name", "?")
            try:
                order_id = str(order["id"])
                if order_exists(order_id):
                    refresh_existing_order_statuses(order)
                    updated += 1
                    continue

                customer    = order.get("customer") or {}
                customer_id = find_customer(customer.get("phone"), customer.get("email"))

                if not customer_id:
                    customer_id = create_customer({
                        "name":    f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip(),
                        "email":   customer.get("email"),
                        "phone":   customer.get("phone"),
                        "address": (order.get("shipping_address") or {}).get("address1")
                    })

                if not customer_id:
                    print(f"❌ {order_name} — could not find/create customer", flush=True)
                    failed += 1
                    continue

                order_record_id = create_order_record(order, customer_id)
                if not order_record_id:
                    failed += 1
                    continue

                create_order_line_items(order, customer_id, order_record_id)
                print(f"✅ {order_name} synced", flush=True)
                synced += 1

            except Exception as e:
                print(f"❌ {order_name} — error: {e}", flush=True)
                failed += 1

        print(
            f"🎉 Sync complete: total={len(all_orders)} synced={synced} "
            f"updated={updated} failed={failed}",
            flush=True
        )

    except Exception as e:
        print(f"❌ Background sync crashed: {e}", flush=True)

    finally:
        with _sync_lock:
            _sync_running = False


@app.route("/sync", methods=["GET"])
def sync_all_orders():
    global _sync_running

    if not SHOPIFY_STORE or not SHOPIFY_TOKEN:
        return jsonify({
            "status":  "error",
            "message": "SHOPIFY_STORE or SHOPIFY_TOKEN env variable not set in Render"
        }), 500

    with _sync_lock:
        if _sync_running:
            return jsonify({
                "status":  "already_running",
                "message": "A sync is already running. Watch Render logs for progress."
            }), 409
        _sync_running = True

    print("🔄 Manual sync triggered — running in background...", flush=True)
    threading.Thread(target=_do_full_sync, daemon=True).start()

    return jsonify({
        "status":  "started",
        "message": "Sync started in background. Watch Render logs for progress. Look for '🎉 Sync complete' when done."
    }), 202


# ---------------- HEALTH CHECK ----------------
@app.route("/health", methods=["GET"])
def health():
    return "ok", 200
