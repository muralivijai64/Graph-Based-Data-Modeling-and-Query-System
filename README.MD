An end-to-end system that transforms fragmented enterprise data into a **connected graph** and enables users to query it using **natural language**.

This project integrates **LLMs (Groq - LLaMA 3.1)** with structured data to generate SQL dynamically and return **data-backed answers along with interactive graph visualization**.

---

## 🚀 Overview

This system converts SAP Order-to-Cash (O2C) data (Orders, Deliveries, Billing, Products) into a **graph-based representation** and allows users to explore relationships using natural language queries.

---

## 🏗️ Architecture

The system is divided into three layers:

### 1️⃣ Data Layer (SQLite)

* Raw JSON data is cleaned and normalized
* Stored in structured tables:

  * `orders`
  * `order_products`
  * `order_delivery`
  * `delivery_billing`

---

### 2️⃣ Backend (FastAPI)

* Accepts natural language queries
* Uses LLM to generate SQL dynamically
* Executes SQL on SQLite database
* Builds graph using NetworkX + PyVis
* Returns:

  * Generated SQL query
  * Query result
  * Natural language answer
  * Graph visualization

---

### 3️⃣ Frontend (HTML + JavaScript)

* Chat-like interface
* Displays SQL, results, and answers
* Dynamically loads graph visualization

---

## 🔁 System Flow

User Query → LLM → SQL → SQLite → Result → Graph Generation → UI

---

## 🧠 LLM Integration

* Provider: Groq
* Model: `llama-3.1-8b-instant`
* Converts natural language → SQL

### Prompt Engineering:

* Restricts schema usage
* Outputs only SQL
* Avoids hallucinations

---

## 🕸️ Graph Modeling

### Nodes:

* Customers
* Orders
* Products
* Deliveries
* Billing Documents
* Journal Entries

### Edges:

* Customer → Order
* Order → Product
* Order → Delivery
* Delivery → Billing
* Billing → Journal

### Features:

* Graph generated dynamically per query
* Relevant nodes are highlighted
* Interactive zoom & drag

---

## 🛡️ Guardrails

* Only dataset-related queries allowed
* Keyword filtering:

  * order, delivery, billing, product, customer

Invalid queries return:

> "This system only supports dataset-related queries."

---

## 📦 Installation

### 1. Clone Repository

```bash
git clone https://github.com/muralivijai64/Graph-Based-Data-Modeling-and-Query-System.git
cd Graph-Based-Data-Modeling-and-Query-System
```

---

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 🔐 Environment Variables (API Key Setup)

### Step 1: Create `.env` file inside backend

```
backend/.env
```

### Step 2: Add your API key

```
GROQ_API_KEY=your_api_key_here
```

⚠️ Important:

* `.env` is ignored by Git
* Never upload your API key

---

## 🗄️ Dataset Setup

Download the SAP Order-to-Cash dataset and place it locally.


---

## ⚙️ Create Database (`data.db`)

Run the following command:

```bash
python prepare.py --data_dir //Path of Downloaded Database//
```

### ✅ What this does:

* Reads raw `.jsonl` files
* Cleans and processes data
* Creates `backend/data.db`

---

## ▶️ Run the Application

### 1. Start Backend

```bash
cd backend
uvicorn main:app --reload
```

---

### 2. Open Frontend

Open:

```
frontend/index.html
```

(or use Live Server in VS Code)

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

## 🧠 Tech Stack

* Python
* FastAPI
* NetworkX
* PyVis
* SQLite
* Groq API (LLM)

---

## 🤖 AI Usage

This project was developed using multiple AI tools:

* **ChatGPT**: Backend logic, debugging, graph modeling, system design
* **Claude**: Frontend UI structuring and HTML improvements

AI was used as an **iterative development assistant** for:

* Designing system architecture
* Debugging runtime issues
* Improving prompt engineering
* Enhancing visualization and UI

---

## ⚠️ Notes

* `.env` and `data/` are excluded via `.gitignore`
* Ensure `data.db` is created before running backend
* Large datasets are not pushed to GitHub

---

## 🔮 Future Improvements

* Replace SQLite with Neo4j (graph DB)
* Add semantic search
* Improve UI with React
* Add streaming responses
* Advanced graph clustering

---

## 📌 Conclusion

This project demonstrates how LLMs can be integrated with structured data and graph modeling to create an **interactive, explainable query system**.

It highlights:

* Strong architectural design
* Practical AI integration
* Real-world data modeling

---

## 👨‍💻 Author

Muralitharan S

---

## ⭐ Support

If you like this project, give it a star ⭐ on GitHub!
