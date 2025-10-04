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
    # timestamp = req.headers.get("X-Slack-Request-Timestamp")
    timestamp = time.time()
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

    logging.error("SIG: %s, %s", my_signature, slack_signature)

    return hmac.compare_digest(my_signature, slack_signature)



@app.route("/slack/command", methods=["POST", "GET"])
def slack_command():

    logging.error("VER: %s", verify_slack_request(request))
    # if not verify_slack_request(request):
    #    abort(400, "Invalid request signature")

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
    
    
    # Kick off async worker
    threading.Thread(
        target=handle_databricks_request,
        args=(app, response_url, text),
        daemon=True,
    ).start()

    return jsonify({
        "response_type": "in_channel",
        "text": f"Olá <@{user_name}>! Sua pergunta foi recebida com sucesso. Será respondida aqui quando estiver pronta!"
    }), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)






