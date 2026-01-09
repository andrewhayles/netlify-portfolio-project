import snowflake.connector
import requests
import json
import base64
from email.message import EmailMessage

# ================= CONFIGURATION =================
# 1. SNOWFLAKE CREDENTIALS
SF_USER = 'ahayles'
SF_PASSWORD = 'c?=...' #(Actual credentials ommitted)
SF_ACCOUNT = 'kdd...' #(Actual credentials ommitted)
SF_DATABASE = '...' #(Actual credentials ommitted)
SF_SCHEMA = '...' #(Actual credentials ommitted)
SF_WAREHOUSE = '...' #(Actual credentials ommitted)

# 2. GOOGLE CREDENTIALS
GOOGLE_CLIENT_ID = "725..." #(Actual credentials ommitted)
GOOGLE_CLIENT_SECRET = "GO..." #(Actual credentials ommitted)
GOOGLE_REFRESH_TOKEN = "1//04..." #(Actual credentials ommitted)
# =================================================

def get_gmail_service_token():
    """Exchanges the refresh token for a fresh access token."""
    token_url = "https://oauth2.googleapis.com/token"
    payload = {
        'client_id': GOOGLE_CLIENT_ID,
        'client_secret': GOOGLE_CLIENT_SECRET,
        'refresh_token': GOOGLE_REFRESH_TOKEN,
        'grant_type': 'refresh_token'
    }
    response = requests.post(token_url, data=payload)
    data = response.json()
    
    if 'access_token' not in data:
        raise Exception(f"Google Auth Failed: {data}")
    
    return data['access_token']

def create_draft(access_token, lead_email, email_subject, email_body):
    """Sends the draft payload to Gmail API."""
    url = "https://gmail.googleapis.com/gmail/v1/users/me/drafts"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    message = EmailMessage()
    message.set_content(email_body)
    message['To'] = lead_email
    message['Subject'] = email_subject
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    
    payload = {'message': {'raw': encoded_message}}
    
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    return response

def main():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )
    cursor = conn.cursor()

    # 1. Fetch pending emails
    print("Checking for pending emails...")
    cursor.execute("SELECT ID, LEAD_EMAIL, EMAIL_SUBJECT, EMAIL_BODY FROM EMAIL_DRAFTS WHERE STATUS = 'PENDING'")
    rows = cursor.fetchall()
    
    if not rows:
        print("No pending emails found.")
        return

    print(f"Found {len(rows)} emails. getting Google Token...")
    
    try:
        access_token = get_gmail_service_token()
    except Exception as e:
        print(e)
        return

    # 2. Process each row
    for row in rows:
        row_id, lead_email, email_subject, email_body = row
        print(f"Creating draft for {lead_email}...")
        
        try:
            resp = create_draft(access_token, lead_email, email_subject, email_body)
            
            if resp.status_code == 200:
                # 3. Update Snowflake on success
                cursor.execute(f"UPDATE EMAIL_DRAFTS SET STATUS = 'DRAFT_CREATED' WHERE ID = '{row_id}'")
                print(" -> Success")
            else:
                print(f" -> Failed: {resp.text}")
                
        except Exception as e:
            print(f" -> Error: {e}")

    print("Done!")
    conn.close()

if __name__ == "__main__":
    main()