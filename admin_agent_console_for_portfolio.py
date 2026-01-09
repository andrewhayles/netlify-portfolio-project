import streamlit as st
import snowflake.connector
import pandas as pd
from pydantic import BaseModel, Field
from langchain_google_genai import ChatGoogleGenerativeAI

# --- PAGE CONFIG ---
st.set_page_config(page_title="Netlify Analysis: Smart Agent", page_icon="🤖", layout="wide")
st.title("🤖 Intelligent Agent (LangChain + Snowflake)")
st.markdown("""
    **Architecture:** `Unstructured Logs` -> `LangChain (Gemini)` -> `Structured Output` -> `Snowflake`
    
    This tool uses Generative AI to analyze behavior and draft hyper-personalized emails.
""")

    
# 1. INITIALIZE SESSION STATE
# This acts as the app's "Short Term Memory"
if "agent_result" not in st.session_state:
    st.session_state.agent_result = None

# --- SIDEBAR: CREDENTIALS ---
with st.sidebar:
    st.header("1. API Keys")
    google_api_key = st.text_input("Google AI API Key", type="password")
    
    st.header("2. Snowflake Config")
    account = st.text_input("Account Identifier", value="your-account-id")
    user = st.text_input("Username", value="your-username")
    password = st.text_input("Snowflake Password", type="password")
    warehouse = st.text_input("Warehouse", value="COMPUTE_WH")
    database = st.text_input("Database", value="DEMO_DB")
    schema = st.text_input("Schema", value="RAW_DATA")
    


# --- 2. DEFINE STRUCTURED OUTPUT (The "Guardrails") ---
# This ensures the AI always gives us data that fits our SQL table
class AgentDecision(BaseModel):
    category: str = Field(description="The user segment: 'Enterprise', 'Growth', 'Support Risk', etc.")
    propensity_score: float = Field(description="A score between 0.0 and 1.0 indicating likelihood to convert or churn.")
    email_subject: str = Field(description="A catchy, relevant email subject line.")
    email_body: str = Field(description="The full email body text, formatted professionally.")
    reasoning: str = Field(description="A brief explanation of why this category was chosen.")

# --- 3. THE LANGCHAIN AGENT ---
def run_smart_analysis(org_name, logs_list, api_key):
    if not api_key:
        return None
    
    # 1. TUNING: Set Temperature to 0.0 for maximum consistency
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-pro",
        google_api_key=api_key,
        temperature=0.0  # <--- CHANGED FROM 0.3
    )
    
    structured_llm = llm.with_structured_output(AgentDecision)
    
    logs_text = "\n".join(logs_list)
    
    # 2. PROMPT ENGINEERING: Add "Conflict Resolution" and "Step-by-Step" logic
    prompt = f"""
    You are a Senior Data Analyst for Netlify.
    
    Analyze the following raw web logs for the user/company '{org_name}'.
    
    RAW LOGS:
    {logs_text}
    
    ANALYSIS RULES (Follow these strictly):
    1. **Identify Mixed Signals**: Look for BOTH positive signs (Pricing, SSO, API docs) and negative signs (Errors, Timeouts).
    2. **Conflict Resolution**:
       - IF user has Enterprise keywords (SSO, SAML) BUT also has Errors -> Categorize as "High Value Support Risk". (They are valuable but frustrated).
       - IF user has ONLY Errors -> Categorize as "Churn Risk".
       - IF user has ONLY Pricing/Docs -> Categorize as "Growth Opportunity".
    3. **Holistic Conclusion**: Do not ignore data. Weigh the *value* of the intent against the *severity* of the errors.
    
    TASK:
    - Assign a propensity score based on the *potential value* of the customer, even if they are currently having errors.
    - Draft an email that acknowledges the specific context. (e.g., "I see you are setting up SSO but hitting timeouts...")
    """
    
    result = structured_llm.invoke(prompt)
    return result

# --- UI: INPUTS ---
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("Simulation Context")
    sim_user_email = st.text_input("User Email", value="andyhayles@gmail.com")
    sim_org_name = st.text_input("Organization", value="Hayles Data Corp")
    
    # --- TOGGLE: SIMULATION VS REALITY ---
    data_source = st.radio("Log Source:", ["Use Simulation Data", "Read from Local File"])
    
    if data_source == "Use Simulation Data":
        # OPTION A: The Dropdown (Good for Demos)
        sim_logs = st.multiselect(
            "Inject Web Logs:",
            [
                "/docs/api-v2/rate-limits",
                "/pricing/enterprise",
                "/security/sso-implementation",
                "/error/500-build-timeout",
                "/billing/invoice-history",
                "/blog/nextjs-middleware"
            ],
            default=["/docs/api-v2/rate-limits", "/pricing/enterprise"]
        )
        
    else:
        # OPTION B: The Real File (Your "Bone Structure")
        # Note: We use the LINUX path, not the Windows \\wsl... path
        file_path = st.text_input("File Path:", value="/home/desja/airflow/dags/extracted_logs.json")
        
        sim_logs = [] # Default to empty
        if st.button("📂 Load File"):
            try:
                with open(file_path, "r") as f:
                    # Assuming the file is line-separated logs or raw text
                    content = f.read() 
                    # Split by lines to make a list
                    sim_logs = content.splitlines() 
                    st.success(f"Loaded {len(sim_logs)} log lines!")
                    st.caption(f"Preview: {sim_logs[:3]}") # Show first 3 lines
            except FileNotFoundError:
                st.error(f"File not found at: {file_path}")

with col2:
    st.subheader("Agent Output")
    
    # 1. SEPARATE THE TRIGGER FROM THE DISPLAY
    if st.button("✨ Generate Analysis"):
        if not google_api_key:
            st.error("Please provide a Google API Key.")
        else:
            with st.spinner("Consulting LLM..."):
                # Save the result to Session State instead of a local variable
                st.session_state.agent_result = run_smart_analysis(sim_org_name, sim_logs, google_api_key)

    # 2. DISPLAY & SAVE (Outside the Generate button block)
    if st.session_state.agent_result:
        decision = st.session_state.agent_result
        
        st.success(f"**Category:** {decision.category}")
        st.info(f"**Reasoning:** {decision.reasoning}")
        st.metric("Propensity Score", f"{int(decision.propensity_score * 100)}%")
        
        with st.expander("View Draft Email", expanded=True):
            st.markdown(f"**Subject:** {decision.email_subject}")
            st.text_area("Body", decision.email_body, height=200)

        # 3. NOW THIS BUTTON WILL WORK
        if st.button("💾 Save to Snowflake"):
            try:
                # ... (Your existing connection code) ...
                ctx = snowflake.connector.connect(
                    user=user,
                    password=password,
                    account=account,
                    warehouse=warehouse,
                    database=database,
                    schema=schema
                )
                cs = ctx.cursor()
                
                sql = """
                INSERT INTO EMAIL_DRAFTS 
                (LEAD_EMAIL, LEAD_NAME, CATEGORY, PROPENSITY_SCORE, EMAIL_SUBJECT, EMAIL_BODY)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                cs.execute(sql, (
                    sim_user_email, 
                    sim_org_name, 
                    decision.category, 
                    decision.propensity_score, 
                    decision.email_subject, 
                    decision.email_body
                ))
                
                ctx.commit() # Vital!
                
                st.toast("Saved to Warehouse!", icon="❄️")
                cs.close()
                ctx.close()
            except Exception as e:
                st.error(f"Database Error: {e}")
                            
# ==========================================
# 🛠️ DEBUGGING & TROUBLESHOOTING SECTION
# ==========================================
st.divider()
st.subheader("🛠️ Database Troubleshooter")

if st.checkbox("Show Connection Test Tools"):
    import random  # Local import to ensure it works instantly
    
    st.info(f"Targeting: Database=`{database}` | Schema=`{schema}` | Table=`EMAIL_DRAFTS`")
    
    if st.button("🔴 Run Connection & Write Test"):
        if not password:
            st.error("⚠️ Please enter your Snowflake password above first.")
        else:
            try:
                # 1. Establish a separate debug connection
                debug_ctx = snowflake.connector.connect(
                    user=user,
                    password=password,
                    account=account,
                    warehouse=warehouse,
                    database=database,
                    schema=schema
                )
                debug_cs = debug_ctx.cursor()
                
                # 2. Verify exactly where we are connected
                debug_cs.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()")
                curr_db, curr_schema, curr_role = debug_cs.fetchone()
                st.write(f"**Snowflake Context:** Connected to `{curr_db}.{curr_schema}` using role `{curr_role}`")
                
                # 3. Attempt a Write
                test_email = f"debug_test_{random.randint(1000,9999)}@localhost.com"
                st.write(f"Attempting to insert test row for: `{test_email}`...")
                
                # We use the standard columns from your CREATE TABLE statement
                debug_query = """
                    INSERT INTO EMAIL_DRAFTS 
                    (LEAD_EMAIL, LEAD_NAME, CATEGORY, PROPENSITY_SCORE, EMAIL_SUBJECT, EMAIL_BODY)
                    VALUES (%s, 'DEBUG_USER', 'TEST', 0.0, 'Debug Subject', 'Debug Body')
                """
                debug_cs.execute(debug_query, (test_email,))
                
                # 4. COMMIT (The Critical Check)
                debug_ctx.commit()
                st.success("✅ Commit sent to Snowflake.")
                
                # 5. Read back to verify it actually saved
                debug_cs.execute(f"SELECT COUNT(*) FROM EMAIL_DRAFTS WHERE LEAD_EMAIL = '{test_email}'")
                row_count = debug_cs.fetchone()[0]
                
                if row_count > 0:
                    st.balloons()
                    st.success(f"🎉 VERIFIED! Found {row_count} row(s) for {test_email}. The pipeline is working!")
                    st.markdown(f"**Diagnosis:** If you see this green success message, your previous issue was likely writing to the wrong Schema (e.g., `PUBLIC` vs `RAW_DATA`). Check the **Snowflake Context** line above carefully.")
                else:
                    st.error("❌ WRITE FAILED: The INSERT command ran without error, but the data is gone. This usually means the table exists in a DIFFERENT schema than the one you are connected to.")
                
                debug_cs.close()
                debug_ctx.close()
                
            except Exception as e:
                st.error(f"🚨 System Error: {e}")