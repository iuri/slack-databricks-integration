import os
import time
import requests
import dotenv
import logging
from flask import jsonify

logging.basicConfig(level=logging.DEBUG)

dotenv.load_dotenv()

DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
DATABRICKS_URL = os.environ.get("DATABRICKS_URL")
SPACE_ID = os.environ.get("SPACE_ID")

def start_conversation(space_id: str, text: str = "") -> dict:
    """Send a request to Databricks Genie to start a conversation."""
    url = f"{DATABRICKS_URL}/spaces/{space_id}/start-conversation"
    # Forward data to Databricks Genie API                                      
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "content": text
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    resp.raise_for_status()
    # print("HEADERS", resp.headers)
    # print("response: ", resp.__dict__)
    return resp.json()



def poll_conversation(space_id: str, conversation_id: str, message_id: str, interval: int = 30, max_attempts: int = 20):
    """Poll Databricks Genie until status == COMPLETED or attempts exhausted."""
    url = f"{DATABRICKS_URL}/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    for attempt in range(max_attempts):
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        status = data.get("status")
        print(f"[Attempt {attempt+1}] Status: {status}")

        if status == "COMPLETED":
            print("✅ Conversation completed.")
            return data
        elif status == "FAILED":
            print("❌ Conversation failed.")
            return data

        time.sleep(interval)

    raise TimeoutError("Conversation did not complete within max attempts")


def conversation_results(space_id: str, conversation_id: str, message_id: str, attachment_id: str, interval: int = 30, max_attempts: int = 20):
    """Fetch and return the results of the conversation."""
    url = f"{DATABRICKS_URL}/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result/{attachment_id}"
        
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    # print("Final data: ", data)
    return data


def handle_databricks_request(app, response_url: str, text: str):
    """Process the Databricks request asynchronously."""
    with app.app_context():

        # Step 1: Start a new conversation
        start_resp = start_conversation(SPACE_ID, text)
        # logging.error("Start response: %s", start_resp)
        conv_id = start_resp["conversation_id"]
        msg_id = start_resp["message_id"]
        
        # Step 2: Poll until completed
        resp = poll_conversation(SPACE_ID, conv_id, msg_id)
        # logging.error("Final result: %s", resp)

        # Step 3. Get the result text
        if resp.get("status") == "COMPLETED":
            attachments = resp.get("attachments", [])
            query = attachments[0].get("query", {}) if attachments else {}
            if query:
                attachment_id = attachments[0].get("attachment_id") if attachments else None
                # logging.error("Attachment ID: %s", attachment_id)
                resp = conversation_results(SPACE_ID, conv_id, msg_id, attachment_id)
                attachments = resp.get("attachments", [])
                # logging.error("Final data: %s", resp)
                    
            text_json = attachments[0].get("text", {}) if attachments else {}
            logging.error("Text: %s", text_json)
            return text_json
            
    return jsonify({"Status": resp.get("status")})

