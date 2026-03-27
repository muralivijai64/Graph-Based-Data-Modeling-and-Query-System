import os
import sqlite3

import networkx as nx
from pyvis.network import Network

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(SCRIPT_DIR, "data.db")
HTML_PATH  = os.path.join(SCRIPT_DIR, "graph.html")

# ── COLOUR PALETTE ──────────────────────────────────────────────────────────
COLORS = {
    "order":    "#1565c0",   # blue
    "delivery": "#e65100",   # orange
    "billing":  "#c62828",   # red
    "journal":  "#00695c",   # teal
    "customer": "#6a1b9a",   # purple
    "product":  "#2e7d32",   # green
    "payment":  "#f9a825",   # amber
}

HIGHLIGHT_COLOR = {
    "background": "#ffffff",
    "border":     "#7f1d1d",
}


def _node_type(node_id: str) -> str:
    """Infer entity type from the node ID prefix."""
    if node_id.startswith("order_"):
        return "order"
    if node_id.startswith("delivery_"):
        return "delivery"
    if node_id.startswith("billing_"):
        return "billing"
    if node_id.startswith("journal_"):
        return "journal"
    if node_id.startswith("customer_"):
        return "customer"
    if node_id.startswith("product_"):
        return "product"
    if node_id.startswith("payment_"):
        return "payment"
    return "order"


def build_graph() -> nx.DiGraph:
    """Query SQLite and construct a directed networkx graph."""
    conn = sqlite3.connect(DB_PATH)
    G = nx.DiGraph()

    # ── Nodes: Sales Orders ─────────────────────────────────────────────────
    for row in conn.execute(
        "SELECT salesOrder, soldToParty, totalNetAmount, overallDeliveryStatus, "
        "overallOrdReltdBillgStatus, creationDate, transactionCurrency "
        "FROM sales_order_headers"
    ):
        nid = f"order_{row[0]}"
        G.add_node(nid,
            entity_type="order",
            label=row[0],
            title=(
                f"<b>Sales Order</b><br>"
                f"ID: {row[0]}<br>"
                f"Customer: {row[1]}<br>"
                f"Amount: {row[2]:,.2f} {row[6]}<br>"
                f"Delivery Status: {row[3] or '—'}<br>"
                f"Billing Status: {row[4] or '—'}<br>"
                f"Created: {(row[5] or '')[:10]}"
            ),
        )

    # ── Nodes: Customers ────────────────────────────────────────────────────
    for row in conn.execute(
        "SELECT DISTINCT h.soldToParty, b.businessPartnerFullName "
        "FROM sales_order_headers h "
        "LEFT JOIN business_partners b ON h.soldToParty = b.businessPartner"
    ):
        nid = f"customer_{row[0]}"
        name = row[1] or row[0]
        G.add_node(nid,
            entity_type="customer",
            label=name[:20],
            title=f"<b>Customer</b><br>ID: {row[0]}<br>Name: {name}",
        )
        # Customer → Order edge
        G.add_edge(nid, f"order_{row[0]}_placeholder")  # will be resolved below

    # Re-add customer→order edges properly
    G.remove_nodes_from([n for n in G.nodes if n.endswith("_placeholder")])
    for row in conn.execute(
        "SELECT salesOrder, soldToParty FROM sales_order_headers"
    ):
        G.add_edge(f"customer_{row[1]}", f"order_{row[0]}")

    # ── Nodes: Products (only those referenced in order items) ──────────────
    for row in conn.execute(
        "SELECT DISTINCT i.material, p.productType, p.productGroup, p.baseUnit "
        "FROM sales_order_items i "
        "LEFT JOIN products p ON i.material = p.product"
    ):
        nid = f"product_{row[0]}"
        if nid not in G.nodes:
            G.add_node(nid,
                entity_type="product",
                label=str(row[0])[-10:],
                title=(
                    f"<b>Product</b><br>"
                    f"ID: {row[0]}<br>"
                    f"Type: {row[1] or '—'}<br>"
                    f"Group: {row[2] or '—'}<br>"
                    f"Unit: {row[3] or '—'}"
                ),
            )

    # Order → Product edges (via order items)
    for row in conn.execute(
        "SELECT DISTINCT salesOrder, material FROM sales_order_items"
    ):
        src = f"order_{row[0]}"
        dst = f"product_{row[1]}"
        if src in G.nodes and dst in G.nodes:
            G.add_edge(src, dst)

    # ── Nodes: Deliveries ───────────────────────────────────────────────────
    for row in conn.execute(
        "SELECT deliveryDocument, creationDate, overallGoodsMovementStatus, "
        "overallPickingStatus, shippingPoint FROM outbound_delivery_headers"
    ):
        nid = f"delivery_{row[0]}"
        G.add_node(nid,
            entity_type="delivery",
            label=row[0],
            title=(
                f"<b>Delivery</b><br>"
                f"ID: {row[0]}<br>"
                f"Goods Movement: {row[2] or '—'}<br>"
                f"Picking: {row[3] or '—'}<br>"
                f"Shipping Point: {row[4] or '—'}<br>"
                f"Created: {(row[1] or '')[:10]}"
            ),
        )

    # Order → Delivery edges (via delivery items)
    for row in conn.execute(
        "SELECT DISTINCT referenceSdDocument, deliveryDocument "
        "FROM outbound_delivery_items"
    ):
        src = f"order_{row[0]}"
        dst = f"delivery_{row[1]}"
        if src in G.nodes and dst in G.nodes:
            G.add_edge(src, dst)

    # ── Nodes: Billing Documents ─────────────────────────────────────────────
    for row in conn.execute(
        "SELECT billingDocument, soldToParty, totalNetAmount, creationDate, "
        "accountingDocument, billingDocumentIsCancelled, transactionCurrency "
        "FROM billing_document_headers"
    ):
        nid = f"billing_{row[0]}"
        cancelled = " [CANCELLED]" if row[5] else ""
        G.add_node(nid,
            entity_type="billing",
            label=row[0],
            title=(
                f"<b>Billing Doc{cancelled}</b><br>"
                f"ID: {row[0]}<br>"
                f"Customer: {row[1]}<br>"
                f"Amount: {row[2]:,.2f} {row[6]}<br>"
                f"Accounting Doc: {row[4] or '—'}<br>"
                f"Created: {(row[3] or '')[:10]}"
            ),
        )

    # Delivery → Billing edges (via billing items)
    for row in conn.execute(
        "SELECT DISTINCT referenceSdDocument, billingDocument "
        "FROM billing_document_items"
    ):
        src = f"delivery_{row[0]}"
        dst = f"billing_{row[1]}"
        if src in G.nodes and dst in G.nodes:
            G.add_edge(src, dst)

    # ── Nodes: Journal Entries ───────────────────────────────────────────────
    for row in conn.execute(
        "SELECT DISTINCT j.accountingDocument, j.postingDate, "
        "SUM(j.amountInTransactionCurrency), j.customer "
        "FROM journal_entry_items j "
        "GROUP BY j.accountingDocument"
    ):
        nid = f"journal_{row[0]}"
        G.add_node(nid,
            entity_type="journal",
            label=row[0],
            title=(
                f"<b>Journal Entry</b><br>"
                f"Acct Doc: {row[0]}<br>"
                f"Posting Date: {(row[1] or '')[:10]}<br>"
                f"Total Amount: {(row[2] or 0):,.2f}<br>"
                f"Customer: {row[3] or '—'}"
            ),
        )

    # Billing → Journal edges (via accountingDocument)
    for row in conn.execute(
        "SELECT billingDocument, accountingDocument FROM billing_document_headers "
        "WHERE accountingDocument IS NOT NULL AND accountingDocument != ''"
    ):
        src = f"billing_{row[0]}"
        dst = f"journal_{row[1]}"
        if src in G.nodes and dst in G.nodes:
            G.add_edge(src, dst)

    conn.close()
    return G


def save_graph(highlight_nodes: list = None) -> dict:
    """
    Build the graph, render with pyvis, save to graph.html.
    Returns basic stats.
    """
    highlight_nodes = set(highlight_nodes or [])
    G = build_graph()

    net = Network(
         height="100vh",
         width="100vw",
         directed=True,
         bgcolor="#0a0c10",
         font_color="#c9d6e3",
    )

    # Physics options for a clean, readable layout
    net.set_options("""
    {
      "physics": {
        "enabled": true,
        "solver": "forceAtlas2Based",
        "forceAtlas2Based": {
          "gravitationalConstant": -120,
          "centralGravity": 0.01,
          "springLength": 200,
          "springConstant": 0.04,
          "damping": 0.8,
          "avoidOverlap": 1
        },
        "stabilization": { "iterations": 300 }
      },
      "edges": {
        "arrows": { "to": { "enabled": true, "scaleFactor": 0.6 } },
        "color": { "color": "#2d3748", "highlight": "#00e5ff" },
        "width": 1,
        "smooth": { "type": "dynamic" }
      },
      "nodes": {
        "shape": "dot",
        "font": { "size": 10, "color": "#c9d6e3" },
        "borderWidth": 1
      },
      "interaction": {
        "hover": true,
        "tooltipDelay": 100,
        "hideEdgesOnDrag": true
      }
    }
    """)

    for node, attrs in G.nodes(data=True):
        etype = attrs.get("entity_type", "order")
        color = COLORS.get(etype, "#888")
        size = {
            "order": 14, "delivery": 12, "billing": 12,
            "customer": 18, "product": 10, "journal": 10, "payment": 10,
        }.get(etype, 10)

        if node in highlight_nodes:
            net.add_node(
                node,
                label=attrs.get("label", node),
                title=attrs.get("title", node),
                color=HIGHLIGHT_COLOR,
                size=size + 30,
                borderWidth=2,
            )
        else:
            net.add_node(
                node,
                label=attrs.get("label", node),
                title=attrs.get("title", node),
                color={"background": color, "border": color},
                size=size,
                borderWidth=1,
            )

    for u, v in G.edges():
        net.add_edge(u, v)

    net.save_graph(HTML_PATH)
    print(f"✅ Graph saved → {HTML_PATH} ({G.number_of_nodes()} nodes, {G.number_of_edges()} edges)")
    return {"nodes": G.number_of_nodes(), "edges": G.number_of_edges()}


# ── Run manually ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    stats = save_graph()
    print(stats)