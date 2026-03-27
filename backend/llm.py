"""
llm.py — Translates natural-language questions into SQLite SQL via Groq.

Reads GROQ_API_KEY from the .env file (or environment).
"""

import os
import re
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

API_KEY   = os.getenv("GROQ_API_KEY", "")
API_URL   = "https://api.groq.com/openai/v1/chat/completions"
LLM_MODEL = "llama-3.1-8b-instant"

# ── FULL SCHEMA PROMPT ───────────────────────────────────────────────────────

SYSTEM_PROMPT = """
You are an expert SQLite SQL generator for a SAP Order-to-Cash (O2C) dataset.

## DATABASE SCHEMA

### Core Transaction Tables
sales_order_headers(salesOrder, soldToParty, totalNetAmount, overallDeliveryStatus,
    overallOrdReltdBillgStatus, creationDate, transactionCurrency, salesOrderType)

sales_order_items(salesOrder, salesOrderItem, material, netAmount,
    requestedQuantity, requestedQuantityUnit, productionPlant, materialGroup)

outbound_delivery_headers(deliveryDocument, creationDate,
    overallGoodsMovementStatus, overallPickingStatus, shippingPoint)

outbound_delivery_items(deliveryDocument, deliveryDocumentItem,
    referenceSdDocument, actualDeliveryQuantity, plant, storageLocation)

billing_document_headers(billingDocument, soldToParty, totalNetAmount,
    creationDate, accountingDocument, billingDocumentIsCancelled,
    cancelledBillingDocument, transactionCurrency, billingDocumentType)

billing_document_items(billingDocument, billingDocumentItem, material,
    netAmount, billingQuantity, referenceSdDocument)

journal_entry_items(accountingDocument, accountingDocumentItem,
    referenceDocument, glAccount, amountInTransactionCurrency,
    postingDate, customer, fiscalYear, companyCode,
    clearingDate, clearingAccountingDocument)

payments(accountingDocument, accountingDocumentItem, customer,
    amountInTransactionCurrency, postingDate,
    clearingAccountingDocument, glAccount)

### Reference / Master Data Tables
business_partners(businessPartner, customer, businessPartnerFullName,
    businessPartnerCategory, creationDate)

products(product, productType, productGroup, baseUnit, grossWeight, weightUnit)

billing_document_cancellations(billingDocument, billingDocumentType,
    creationDate, totalNetAmount, accountingDocument,
    billingDocumentIsCancelled, soldToParty)

## KEY RELATIONSHIPS (JOIN PATHS)
1. Sales Order → Delivery:
   outbound_delivery_items.referenceSdDocument = sales_order_headers.salesOrder

2. Delivery → Billing:
   billing_document_items.referenceSdDocument = outbound_delivery_headers.deliveryDocument

3. Billing → Journal Entry:
   billing_document_headers.accountingDocument = journal_entry_items.accountingDocument

4. Journal → Payment:
   journal_entry_items.accountingDocument = payments.clearingAccountingDocument

5. Customer name:
   sales_order_headers.soldToParty = business_partners.businessPartner

6. Product details:
   sales_order_items.material = products.product

## EXAMPLE JOIN PATTERNS

Full O2C trace (Order → Delivery → Billing → Journal):
SELECT soh.salesOrder, odh.deliveryDocument, bdh.billingDocument, je.accountingDocument
FROM sales_order_headers soh
LEFT JOIN outbound_delivery_items odi ON soh.salesOrder = odi.referenceSdDocument
LEFT JOIN outbound_delivery_headers odh ON odi.deliveryDocument = odh.deliveryDocument
LEFT JOIN billing_document_items bdi ON odh.deliveryDocument = bdi.referenceSdDocument
LEFT JOIN billing_document_headers bdh ON bdi.billingDocument = bdh.billingDocument
LEFT JOIN journal_entry_items je ON bdh.accountingDocument = je.accountingDocument
WHERE soh.salesOrder = '740506';

Orders delivered but NOT billed (broken flow):
SELECT DISTINCT soh.salesOrder
FROM sales_order_headers soh
JOIN outbound_delivery_items odi ON soh.salesOrder = odi.referenceSdDocument
LEFT JOIN billing_document_items bdi ON odi.deliveryDocument = bdi.referenceSdDocument
WHERE bdi.billingDocument IS NULL;

Products with most billing documents:
SELECT bdi.material, COUNT(DISTINCT bdi.billingDocument) AS billing_count
FROM billing_document_items bdi
GROUP BY bdi.material
ORDER BY billing_count DESC
LIMIT 10;

## STRICT RULES
- Output ONLY raw SQL, no markdown fences, no explanations, no backticks.
- Use SQLite syntax: LIMIT not TOP, || for string concat.
- Never invent column names — only use columns listed in the schema above.
- Always use table aliases for multi-table queries.
- Use DISTINCT when aggregating across JOINs to avoid row duplication.
- For customer names, JOIN with business_partners on businessPartner = soldToParty.
- For product details, JOIN with products on product = material.
""".strip()


def generate_sql(question: str) -> str:
    """Call Groq LLM to convert a natural-language question to SQL."""
    if not API_KEY:
        return "ERROR: GROQ_API_KEY not set in .env"

    prompt = f"Question: {question}\n\nSQL:"

    try:
        resp = requests.post(
            API_URL,
            headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
            json={
                "model": LLM_MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 512,
            },
            timeout=30,
        )
        data = resp.json()
    except requests.RequestException as exc:
        return f"ERROR: {exc}"

    if "choices" not in data:
        return f"ERROR: {data.get('error', {}).get('message', str(data))}"

    raw = data["choices"][0]["message"]["content"].strip()
    return _clean_sql(raw)


def _clean_sql(sql: str) -> str:
    """Strip markdown fences and extra whitespace."""
    sql = re.sub(r"```sql", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"```",    "", sql)
    # Keep only the first statement (stop at second semicolon)
    parts = sql.split(";")
    sql = parts[0].strip()
    if sql:
        sql += ";"
    return sql