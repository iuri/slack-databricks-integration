import os
import hmac
import time
import dotenv                                                                                                                                                                                               
import hashlib
import logging
import requests
import threading
from flask import Flask, request, jsonify, abort
from databricks import handle_databricks_request

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)

dotenv.load_dotenv()

# Slack signing secret (get from Slack App dashboard)                                                                                                                                                                                          
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET")

def verify_slack_request(req):
    """Verify that the request comes from Slack."""
    timestamp = req.headers.get("X-Slack-Request-Timestamp")
    slack_signature = req.headers.get("X-Slack-Signature")
    # logging.error("Timestamp: %s", timestamp)
    # logging.error("Slack Signature: %s", slack_signature)
  
    if abs(time.time() - int(timestamp)) > 60 * 5:
        # Too old, possible replay attack
        return False
    sig_basestring = f"v0:{timestamp}:{req.get_data(as_text=True)}"
    my_signature = "v0=" + hmac.new(
        SLACK_SIGNING_SECRET.encode(),
        sig_basestring.encode(),
        hashlib.sha256
    ).hexdigest()

    # logging.error("SIG: %s, %s", my_signature, slack_signature)

    return hmac.compare_digest(my_signature, slack_signature)




def format_response(response: dict) -> str:
    try:
        if response.get("manifest", {}):
            columns = [col["name"] for col in response["manifest"]["schema"]["columns"]]
            rows = response["result"]["data_array"]

            # Format as a simple table for Slack
            header = " | ".join(columns)
            divider = "-|-".join(["-" * len(c) for c in columns])
            lines = [header, divider]

            for row in rows:
                line = " | ".join(str(v) for v in row)
                lines.append(line)

            return "```\n" + "\n".join(lines) + "\n```"
        
        else:
            return "```\n" + response.get("content",{}) + "\n```"
        
    except Exception as e:
        return f"⚠️ Could not parse result: {e}"



def send_result_to_slack(response_url: str, response: dict, query: str):
    text_table = format_response(response)

    message = {
        "response_type": "in_channel",
        "text": f"📊 Pergunta: `{query}`:\n{text_table}"
    }

    resp = requests.post(response_url, json=message)
    resp.raise_for_status()
    print("RESP", resp)
    return jsonify(message.get("text"))




@app.route("/slack/command", methods=["POST", "GET"])
def slack_command():

    # logging.error("VER: %s", verify_slack_request(request))
    if not verify_slack_request(request):
        abort(400, "Invalid request signature")

    # Parse DATA safely
    data = request.get_data()
    # logging.error("DATA: %s", data)
    if not data:
        abort(400, "Invalid payload")

    # Parse form data
    token = request.form.get("token")
    team_id = request.form.get("team_id")
    team_domain = request.form.get("team_domain")

    channel_id = request.form.get("channel_id")
    channel_name = request.form.get("channel_name")

    user_id = request.form.get("user_id")
    user_name = request.form.get("user_name")

    command = request.form.get("command")
    text = request.form.get("text")

    api_app_id = request.form.get("api_app_id")
    is_enterprise_install = request.form.get("is_enterprise_install")
    response_url = request.form.get("response_url")
    trigger_id = request.form.get("trigger_id")
    
    # Post back to Slack using response_url
    requests.post(response_url,json={
        "response_type": "in_channel",
        "text": f"Olá <@{user_name}>! Sua pergunta foi recebida com sucesso. Será respondida aqui quando estiver pronta!"
    })

    
    response, query = handle_databricks_request(app, response_url, text)

    print("Response", response)
    print("Query", query)
    if not query:
        query = text

    result = send_result_to_slack(response_url, response, query)
    
    return result, 200
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)






