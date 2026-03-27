        ┌──────────────┐
        │   Frontend   │
        │ (HTML + JS)  │
        └──────┬───────┘
               │
               ▼
      ┌──────────────────┐
      │   FastAPI API    │
      │  /query          │
      │  /graph-file     │
      └──────┬───────────┘
             │
   ┌─────────┴─────────┐
   ▼                   ▼
┌────────────┐   ┌──────────────┐
│    LLM     │   │  SQLite DB   │
│ (Groq API) │   │ Clean Tables │
└────┬───────┘   └──────┬───────┘
     │                  │
     ▼                  ▼
   SQL Query       Query Result
         └──────────────┬──────────────┘
                        ▼
              ┌────────────────┐
              │ Graph Builder  │
              │ NetworkX +     │
              │ PyVis          │
              └──────┬─────────┘
                     ▼
           graph_<timestamp>.html
                     ▼
               Frontend UI