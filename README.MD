# Graph-Based Data Modeling and Query System

## 🚀 Overview

This project builds a **Graph-Based Query System** that transforms fragmented business data (Orders, Deliveries, Billing, Products) into a **connected graph** and enables users to query it using **natural language**.

The system uses an **LLM (Groq - LLaMA 3.1)** to dynamically convert user queries into SQL and returns **data-backed answers along with graph visualization**.

---

## 🏗️ Architecture

The system is divided into three layers:

### 1. Data Layer (SQLite)

* Raw JSON data is cleaned and normalized
* Stored in structured tables:

  * `orders`
  * `order_products`
  * `order_delivery`
  * `delivery_billing`

---

### 2. Backend (FastAPI)

* Accepts natural language queries
* Uses LLM to generate SQL dynamically
* Executes SQL on SQLite database
* Builds graph using NetworkX + PyVis
* Returns:

  * SQL query
  * Result
  * Natural language answer
  * Graph file

---

### 3. Frontend (HTML + JavaScript)

* Simple chat-like interface
* Displays SQL, results, and answer
* Dynamically loads graph visualization

---

## 🔁 System Flow

User Query → LLM → SQL → SQLite → Result → Graph Generation → UI

---

## 🧠 LLM Integration

* Provider: Groq
* Model: `llama-3.1-8b-instant`
* Converts natural language → SQL
* Prompt engineered to:

  * Restrict schema usage
  * Output only SQL
  * Avoid hallucinations

---

## 🕸️ Graph Modeling

### Nodes:

* Orders
* Products
* Deliveries
* Billing Documents

### Edges:

* Order → Product
* Order → Delivery
* Delivery → Billing

Graphs are:

* Generated dynamically per query
* Highlighted based on query results

---

## 🛡️ Guardrails

* Only dataset-related queries allowed
* Keyword filtering:

  * order, delivery, billing, product, customer

Invalid queries return:

> "This system only supports dataset-related queries."

---

## ⚙️ Setup Instructions

### 1. Clone Repository

```bash
git clone <your-repo-link>
cd graph-ai-system
```

### 2. Backend Setup

```bash
cd backend
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run Server

```bash
uvicorn main:app --reload
```

### 4. Open UI

Open:

```text
frontend/index.html
```

---

## 📊 Example Queries

* top 5 products
* orders without delivery
* trace full flow for an order

---

## ⭐ Features

* Natural Language → SQL using LLM
* Dynamic Graph Visualization
* Node Highlighting based on results
* Guardrails for domain restriction
* Lightweight and modular architecture

---

## 🤖 AI Usage

This project was developed using multiple AI tools:

* **ChatGPT**: Backend architecture, debugging, SQL generation logic, graph modeling
* **Claude**: Frontend UI structuring and HTML improvements

AI was used as an **iterative development assistant** for:

* Designing system architecture
* Debugging runtime issues (SQLite, FastAPI, threading)
* Improving prompt engineering
* Enhancing UI and visualization

---

## 🔮 Future Improvements

* Replace SQLite with Neo4j (graph DB)
* Add semantic search
* Improve UI with React
* Add streaming responses
* Better graph clustering

---

## 📌 Conclusion

This project demonstrates how LLMs can be integrated with structured data and graph modeling to create an interactive and explainable query system.

It highlights strong architectural thinking, effective AI usage, and practical system design.
