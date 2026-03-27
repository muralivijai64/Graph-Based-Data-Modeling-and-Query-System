"""
setup_db.py — Run this ONCE to build data.db from the extracted dataset.

Usage:
    python setup_db.py --data_dir /path/to/sap-o2c-data

The script reads all JSONL files from the dataset, creates normalized SQLite
tables, and saves data.db next to this script.
"""

import argparse
import glob
import json
import os
import sqlite3

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "data.db")


# ── SCHEMA ──────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS sales_order_headers (
    salesOrder                   TEXT PRIMARY KEY,
    soldToParty                  TEXT,
    totalNetAmount               REAL,
    overallDeliveryStatus        TEXT,
    overallOrdReltdBillgStatus   TEXT,
    creationDate                 TEXT,
    transactionCurrency          TEXT,
    salesOrderType               TEXT,
    salesOrganization            TEXT
);

CREATE TABLE IF NOT EXISTS sales_order_items (
    salesOrder            TEXT,
    salesOrderItem        TEXT,
    material              TEXT,
    netAmount             REAL,
    requestedQuantity     REAL,
    requestedQuantityUnit TEXT,
    productionPlant       TEXT,
    materialGroup         TEXT,
    PRIMARY KEY (salesOrder, salesOrderItem)
);

CREATE TABLE IF NOT EXISTS outbound_delivery_headers (
    deliveryDocument            TEXT PRIMARY KEY,
    creationDate                TEXT,
    overallGoodsMovementStatus  TEXT,
    overallPickingStatus        TEXT,
    shippingPoint               TEXT
);

CREATE TABLE IF NOT EXISTS outbound_delivery_items (
    deliveryDocument       TEXT,
    deliveryDocumentItem   TEXT,
    referenceSdDocument    TEXT,   -- = salesOrder
    actualDeliveryQuantity REAL,
    plant                  TEXT,
    storageLocation        TEXT,
    PRIMARY KEY (deliveryDocument, deliveryDocumentItem)
);

CREATE TABLE IF NOT EXISTS billing_document_headers (
    billingDocument             TEXT PRIMARY KEY,
    soldToParty                 TEXT,
    totalNetAmount              REAL,
    creationDate                TEXT,
    accountingDocument          TEXT,
    billingDocumentIsCancelled  INTEGER,
    cancelledBillingDocument    TEXT,
    transactionCurrency         TEXT,
    billingDocumentType         TEXT,
    fiscalYear                  TEXT
);

CREATE TABLE IF NOT EXISTS billing_document_items (
    billingDocument      TEXT,
    billingDocumentItem  TEXT,
    material             TEXT,
    netAmount            REAL,
    billingQuantity      REAL,
    referenceSdDocument  TEXT,   -- = deliveryDocument
    PRIMARY KEY (billingDocument, billingDocumentItem)
);

CREATE TABLE IF NOT EXISTS journal_entry_items (
    accountingDocument          TEXT,
    accountingDocumentItem      TEXT,
    referenceDocument           TEXT,   -- = billingDocument (cancellation type)
    glAccount                   TEXT,
    amountInTransactionCurrency REAL,
    postingDate                 TEXT,
    customer                    TEXT,
    fiscalYear                  TEXT,
    companyCode                 TEXT,
    clearingDate                TEXT,
    clearingAccountingDocument  TEXT,
    PRIMARY KEY (accountingDocument, accountingDocumentItem)
);

CREATE TABLE IF NOT EXISTS payments (
    accountingDocument          TEXT,
    accountingDocumentItem      TEXT,
    customer                    TEXT,
    amountInTransactionCurrency REAL,
    postingDate                 TEXT,
    clearingAccountingDocument  TEXT,
    glAccount                   TEXT,
    PRIMARY KEY (accountingDocument, accountingDocumentItem)
);

CREATE TABLE IF NOT EXISTS business_partners (
    businessPartner         TEXT PRIMARY KEY,
    customer                TEXT,
    businessPartnerFullName TEXT,
    businessPartnerCategory TEXT,
    creationDate            TEXT
);

CREATE TABLE IF NOT EXISTS products (
    product      TEXT PRIMARY KEY,
    productType  TEXT,
    productGroup TEXT,
    baseUnit     TEXT,
    grossWeight  REAL,
    weightUnit   TEXT
);

CREATE TABLE IF NOT EXISTS billing_document_cancellations (
    billingDocument            TEXT PRIMARY KEY,
    billingDocumentType        TEXT,
    creationDate               TEXT,
    totalNetAmount             REAL,
    accountingDocument         TEXT,
    billingDocumentIsCancelled INTEGER,
    soldToParty                TEXT
);

-- Indexes for fast JOIN performance
CREATE INDEX IF NOT EXISTS idx_soi_order   ON sales_order_items(salesOrder);
CREATE INDEX IF NOT EXISTS idx_soi_mat     ON sales_order_items(material);
CREATE INDEX IF NOT EXISTS idx_odi_ref     ON outbound_delivery_items(referenceSdDocument);
CREATE INDEX IF NOT EXISTS idx_odi_del     ON outbound_delivery_items(deliveryDocument);
CREATE INDEX IF NOT EXISTS idx_bdi_ref     ON billing_document_items(referenceSdDocument);
CREATE INDEX IF NOT EXISTS idx_bdi_bil     ON billing_document_items(billingDocument);
CREATE INDEX IF NOT EXISTS idx_bdh_acct    ON billing_document_headers(accountingDocument);
CREATE INDEX IF NOT EXISTS idx_je_acct     ON journal_entry_items(accountingDocument);
CREATE INDEX IF NOT EXISTS idx_je_cust     ON journal_entry_items(customer);
CREATE INDEX IF NOT EXISTS idx_pay_cust    ON payments(customer);
CREATE INDEX IF NOT EXISTS idx_soh_party   ON sales_order_headers(soldToParty);
"""


# ── LOADERS ─────────────────────────────────────────────────────────────────

def load_jsonl(folder):
    """Yield every parsed JSON object from all *.jsonl files in folder."""
    for path in glob.glob(os.path.join(folder, "*.jsonl")):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)


def safe_float(v):
    try:
        return float(v) if v not in (None, "", "null") else None
    except (ValueError, TypeError):
        return None


def safe_bool_int(v):
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, int):
        return v
    return None


def insert_many(conn, sql, rows):
    if rows:
        conn.executemany(sql, rows)


# ── TABLE POPULATORS ─────────────────────────────────────────────────────────

def load_sales_order_headers(conn, data_dir):
    rows = []
    for d in load_jsonl(os.path.join(data_dir, "sales_order_headers")):
        rows.append((
            d.get("salesOrder"), d.get("soldToParty"),
            safe_float(d.get("totalNetAmount")),
            d.get("overallDeliveryStatus"), d.get("overallOrdReltdBillgStatus"),
            d.get("creationDate"), d.get("transactionCurrency"),
            d.get("salesOrderType"), d.get("salesOrganization"),
        ))
    insert_many(conn,
        "INSERT OR IGNORE INTO sales_order_headers VALUES (?,?,?,?,?,?,?,?,?)", rows)
    print(f"  sales_order_headers: {len(rows)} rows")


def load_sales_order_items(conn, data_dir):
    rows = []
    for d in load_jsonl(os.path.join(data_dir, "sales_order_items")):
        rows.append((
            d.get("salesOrder"), d.get("salesOrderItem"), d.get("material"),
            safe_float(d.get("netAmount")), safe_float(d.get("requestedQuantity")),
            d.get("requestedQuantityUnit"), d.get("productionPlant"),
            d.get("materialGroup"),
        ))
    insert_many(conn,
        "INSERT OR IGNORE INTO sales_order_items VALUES (?,?,?,?,?,?,?,?)", rows)
    print(f"  sales_order_items: {len(rows)} rows")


def load_outbound_delivery_headers(conn, data_dir):
    rows = []
    for d in load_jsonl(os.path.join(data_dir, "outbound_delivery_headers")):
        rows.append((
            d.get("deliveryDocument"), d.get("creationDate"),
            d.get("overallGoodsMovementStatus"), d.get("overallPickingStatus"),
            d.get("shippingPoint"),
        ))
    insert_many(conn,
        "INSERT OR IGNORE INTO outbound_delivery_headers VALUES (?,?,?,?,?)", rows)
    print(f"  outbound_delivery_headers: {len(rows)} rows")


def load_outbound_delivery_items(conn, data_dir):
    rows = []
    for d in load_jsonl(os.path.join(data_dir, "outbound_delivery_items")):
        rows.append((
            d.get("deliveryDocument"), d.get("deliveryDocumentItem"),
            d.get("referenceSdDocument"), safe_float(d.get("actualDeliveryQuantity")),
            d.get("plant"), d.get("storageLocation"),
        ))
    insert_many(conn,
        "INSERT OR IGNORE INTO outbound_delivery_items VALUES (?,?,?,?,?,?)", rows)
    print(f"  outbound_delivery_items: {len(rows)} rows")


def load_billing_document_headers(conn, data_dir):
    rows = []
    for d in load_jsonl(os.path.join(data_dir, "billing_document_headers")):
        rows.append((
            d.get("billingDocument"), d.get("soldToParty"),
            safe_float(d.get("totalNetAmount")), d.get("creationDate"),
            d.get("accountingDocument"), safe_bool_int(d.get("billingDocumentIsCancelled")),
            d.get("cancelledBillingDocument"), d.get("transactionCurrency"),
            d.get("billingDocumentType"), d.get("fiscalYear"),
        ))
    insert_many(conn,
        "INSERT OR IGNORE INTO billing_document_headers VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    print(f"  billing_document_headers: {len(rows)} rows")


def load_billing_document_items(conn, data_dir):
    rows = []
    for d in load_jsonl(os.path.join(data_dir, "billing_document_items")):
        rows.append((
            d.get("billingDocument"), d.get("billingDocumentItem"),
            d.get("material"), safe_float(d.get("netAmount")),
            safe_float(d.get("billingQuantity")), d.get("referenceSdDocument"),
        ))
    insert_many(conn,
        "INSERT OR IGNORE INTO billing_document_items VALUES (?,?,?,?,?,?)", rows)
    print(f"  billing_document_items: {len(rows)} rows")


def load_journal_entry_items(conn, data_dir):
    rows = []
    for d in load_jsonl(os.path.join(data_dir, "journal_entry_items_accounts_receivable")):
        rows.append((
            d.get("accountingDocument"), d.get("accountingDocumentItem"),
            d.get("referenceDocument"), d.get("glAccount"),
            safe_float(d.get("amountInTransactionCurrency")),
            d.get("postingDate"), d.get("customer"),
            d.get("fiscalYear"), d.get("companyCode"),
            d.get("clearingDate"), d.get("clearingAccountingDocument"),
        ))
    insert_many(conn,
        "INSERT OR IGNORE INTO journal_entry_items VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    print(f"  journal_entry_items: {len(rows)} rows")


def load_payments(conn, data_dir):
    rows = []
    for d in load_jsonl(os.path.join(data_dir, "payments_accounts_receivable")):
        rows.append((
            d.get("accountingDocument"), d.get("accountingDocumentItem"),
            d.get("customer"), safe_float(d.get("amountInTransactionCurrency")),
            d.get("postingDate"), d.get("clearingAccountingDocument"),
            d.get("glAccount"),
        ))
    insert_many(conn,
        "INSERT OR IGNORE INTO payments VALUES (?,?,?,?,?,?,?)", rows)
    print(f"  payments: {len(rows)} rows")


def load_business_partners(conn, data_dir):
    rows = []
    for d in load_jsonl(os.path.join(data_dir, "business_partners")):
        rows.append((
            d.get("businessPartner"), d.get("customer"),
            d.get("businessPartnerFullName"), d.get("businessPartnerCategory"),
            d.get("creationDate"),
        ))
    insert_many(conn,
        "INSERT OR IGNORE INTO business_partners VALUES (?,?,?,?,?)", rows)
    print(f"  business_partners: {len(rows)} rows")


def load_products(conn, data_dir):
    rows = []
    for d in load_jsonl(os.path.join(data_dir, "products")):
        rows.append((
            d.get("product"), d.get("productType"), d.get("productGroup"),
            d.get("baseUnit"), safe_float(d.get("grossWeight")), d.get("weightUnit"),
        ))
    insert_many(conn,
        "INSERT OR IGNORE INTO products VALUES (?,?,?,?,?,?)", rows)
    print(f"  products: {len(rows)} rows")


def load_billing_document_cancellations(conn, data_dir):
    rows = []
    for d in load_jsonl(os.path.join(data_dir, "billing_document_cancellations")):
        rows.append((
            d.get("billingDocument"), d.get("billingDocumentType"),
            d.get("creationDate"), safe_float(d.get("totalNetAmount")),
            d.get("accountingDocument"), safe_bool_int(d.get("billingDocumentIsCancelled")),
            d.get("soldToParty"),
        ))
    insert_many(conn,
        "INSERT OR IGNORE INTO billing_document_cancellations VALUES (?,?,?,?,?,?,?)", rows)
    print(f"  billing_document_cancellations: {len(rows)} rows")


# ── MAIN ─────────────────────────────────────────────────────────────────────

def build(data_dir):
    if not os.path.isdir(data_dir):
        raise SystemExit(f"ERROR: data_dir not found: {data_dir}")

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed old {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)

    print("Loading tables...")
    load_sales_order_headers(conn, data_dir)
    load_sales_order_items(conn, data_dir)
    load_outbound_delivery_headers(conn, data_dir)
    load_outbound_delivery_items(conn, data_dir)
    load_billing_document_headers(conn, data_dir)
    load_billing_document_items(conn, data_dir)
    load_journal_entry_items(conn, data_dir)
    load_payments(conn, data_dir)
    load_business_partners(conn, data_dir)
    load_products(conn, data_dir)
    load_billing_document_cancellations(conn, data_dir)

    conn.commit()
    conn.close()
    print(f"\n✅ Database built: {DB_PATH}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_dir",
        required=True,
        help="Path to the extracted sap-o2c-data directory",
    )
    args = parser.parse_args()
    build(args.data_dir)