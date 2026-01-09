# Netlify Log Analysis & Automated Outreach Agent

An end-to-end data pipeline that ingests raw server logs, analyzes user behavior using Generative AI (Gemini), and autonomously drafts hyper-personalized outreach emails for the sales team.

## 🏗 Architecture
This project demonstrates a modern "Data Ops" architecture integrating Airflow, Snowflake, and LLMs:

1.  **Ingestion (Airflow):** A daily DAG fetches build logs from the Netlify API and loads them into Snowflake via an internal stage.
2.  **Warehousing (Snowflake):** Structured storage of raw logs (`NETLIFY_LOGS`) and generated email drafts (`EMAIL_DRAFTS`).
3.  **Intelligence (LangChain + Gemini):** An agent analyzes the logs to identify "Churn Risk" vs. "Growth Opportunity" patterns.
4.  **Action (Streamlit):** A frontend UI for admins to review AI-generated drafts before approval.
5.  **Dispatch (Gmail API):** Automated script to create validated drafts in the sales rep's inbox.

## 📂 Key Files
* `netlify_build_logs_dag_for_portfolio.py`: Airflow DAG that handles API extraction and "ELT" loading into Snowflake.
* `admin_agent_console_for_portfolio.py`: Streamlit application acting as the "Human-in-the-loop" control plane. Uses LangChain to structure Gemini's output into strict JSON.
* `snowflake_email_automation_manual_for_portfolio.py`: The execution worker that reads approved rows from Snowflake and pushes them to the Gmail API.

## 🛠 Tech Stack
* **Orchestration:** Apache Airflow
* **Database:** Snowflake (Standard SQL + Variant/JSON support)
* **AI/Agents:** LangChain, Google Gemini 2.0 Flash
* **Frontend:** Streamlit
* **Language:** Python 3.9+

## 🚀 How to Run
1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
2.  **Configure Secrets:**
    * Add your `GOOGLE_API_KEY` and Snowflake credentials to `.env` (not included in repo).
3.  **Launch Dashboard:**
    ```bash
    streamlit run admin_agent_console_for_portfolio.py
    ```