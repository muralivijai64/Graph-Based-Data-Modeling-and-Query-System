"""
main.py — FastAPI backend for GraphMind.

Run with:
    uvicorn main:app --reload

Endpoints:
    GET /           → health check
    GET /query?q=   → NL query → SQL → results + graph highlight
    GET /graph      → rebuild graph, return stats
    GET /graph.html → serve the pyvis HTML (static)
"""

import os
import re
import sqlite3

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from llm import generate_sql
from graph import save_graph

# ── PATHS ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(SCRIPT_DIR, "data.db")
HTML_PATH  = os.path.join(SCRIPT_DIR, "graph.html")

# ── APP ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="GraphMind API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── KNOWN ID SETS (loaded once at startup) ───────────────────────────────────
ORDER_IDS    : set[str] = set()
DELIVERY_IDS : set[str] = set()
BILLING_IDS  : set[str] = set()
JOURNAL_IDS  : set[str] = set()
CUSTOMER_IDS : set[str] = set()
PRODUCT_IDS  : set[str] = set()


@app.on_event("startup")
def _load_id_sets():
    """Pre-load all entity IDs so highlight detection is accurate."""
    if not os.path.exists(DB_PATH):
        return  # DB not built yet — will be empty until setup_db.py is run
    conn = sqlite3.connect(DB_PATH)
    global ORDER_IDS, DELIVERY_IDS, BILLING_IDS, JOURNAL_IDS, CUSTOMER_IDS, PRODUCT_IDS
    ORDER_IDS    = {r[0] for r in conn.execute("SELECT salesOrder FROM sales_order_headers")}
    DELIVERY_IDS = {r[0] for r in conn.execute("SELECT deliveryDocument FROM outbound_delivery_headers")}
    BILLING_IDS  = {r[0] for r in conn.execute("SELECT billingDocument FROM billing_document_headers")}
    JOURNAL_IDS  = {r[0] for r in conn.execute("SELECT DISTINCT accountingDocument FROM journal_entry_items")}
    CUSTOMER_IDS = {r[0] for r in conn.execute("SELECT businessPartner FROM business_partners")}
    PRODUCT_IDS  = {r[0] for r in conn.execute("SELECT DISTINCT material FROM sales_order_items")}
    conn.close()


# ── HELPERS ──────────────────────────────────────────────────────────────────

DOMAIN_KEYWORDS = [
    "order", "delivery", "billing", "invoice", "product", "customer",
    "material", "payment", "journal", "dispatch", "shipment", "sales",
    "business partner", "amount", "flow", "trace", "cancel",
]


def is_valid(question: str) -> bool:
    q = question.lower()
    return any(kw in q for kw in DOMAIN_KEYWORDS)


def clean_sql(sql: str) -> str:
    sql = re.sub(r"```sql", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"```",    "", sql)
    return sql.strip()


def classify_value(val: str) -> str | None:
    """Return graph node ID for a raw string value, or None if unrecognised."""
    if val in ORDER_IDS:
        return f"order_{val}"
    if val in DELIVERY_IDS:
        return f"delivery_{val}"
    if val in BILLING_IDS:
        return f"billing_{val}"
    if val in JOURNAL_IDS:
        return f"journal_{val}"
    if val in CUSTOMER_IDS:
        return f"customer_{val}"
    if val in PRODUCT_IDS:
        return f"product_{val}"
    return None


def build_highlight_set(result: list) -> list:
    """Walk every cell in the SQL result and collect matching graph node IDs."""
    nodes = set()
    for row in result:
        for val in row:
            if val is None:
                continue
            node = classify_value(str(val))
            if node:
                nodes.add(node)
    return list(nodes)


def format_answer(question: str, result: list, sql: str) -> str:
    """Return a short natural-language answer from the SQL result."""
    if not result:
        return "No results found for your query."

    q = question.lower()
    rows = result

    # Single scalar result (COUNT, SUM, etc.)
    if len(rows) == 1 and len(rows[0]) == 1:
        val = rows[0][0]
        if "count" in sql.lower() or "how many" in q:
            return f"There are **{val}** matching records."
        return f"Result: **{val}**"

    # Multi-row single-column → list of values
    if all(len(r) == 1 for r in rows):
        values = [str(r[0]) for r in rows if r[0] is not None]
        if len(values) == 1:
            return f"Found 1 result: **{values[0]}**"
        return f"Found {len(values)} results: " + ", ".join(values[:20]) + (
            f" … and {len(values)-20} more" if len(values) > 20 else ""
        )

    # Multi-column — describe the first few rows
    count = len(rows)
    first_vals = ", ".join(str(v) for v in rows[0] if v is not None)
    if count == 1:
        return f"1 record found: {first_vals}"
    return f"{count} records found. First: {first_vals}"


# ── ROUTES ────────────────────────────────────────────────────────────────────

@app.get("/")
def home():
    return {"status": "GraphMind API running", "db": os.path.exists(DB_PATH)}


@app.get("/graph.html", response_class=FileResponse)
def serve_graph_html():
    """Serve the pyvis graph HTML file directly."""
    if not os.path.exists(HTML_PATH):
        # Build on first request
        save_graph()
    return FileResponse(HTML_PATH, media_type="text/html")


@app.get("/graph")
def rebuild_graph():
    """Trigger a full graph rebuild (no highlights) and return stats."""
    stats = save_graph()
    return {"status": "ok", **stats}


@app.get("/query")
def query(q: str):
    if not q.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    # ── Guardrail ─────────────────────────────────────────────────────────────
    if not is_valid(q):
        return {
            "answer": (
                "This system is designed to answer questions related to "
                "the provided dataset only (orders, deliveries, billing, "
                "customers, products, payments, journal entries)."
            )
        }

    # ── Generate SQL ──────────────────────────────────────────────────────────
    sql = generate_sql(q)
    if sql.startswith("ERROR"):
        return {"error": sql}

    sql = clean_sql(sql)

    # ── Execute SQL ───────────────────────────────────────────────────────────
    conn = sqlite3.connect(DB_PATH)
    try:
        result = conn.execute(sql).fetchall()
    except Exception as exc:
        conn.close()
        return {"error": str(exc), "sql": sql}
    conn.close()

    # ── Highlight matching nodes ───────────────────────────────────────────────
    highlight = build_highlight_set(result)
    save_graph(highlight)

    # ── Format answer ──────────────────────────────────────────────────────────
    answer = format_answer(q, result, sql)

    return {
        "question": q,
        "sql":      sql,
        "result":   result,
        "answer":   answer,
        "highlighted_nodes": len(highlight),
    }