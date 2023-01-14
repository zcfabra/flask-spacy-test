from flask import Flask, request,jsonify, make_response
from flask_cors import CORS
import json
app = Flask(__name__)
import pika

cors = CORS()

connection = pika.BlockingConnection(pika.ConnectionParameters("0.0.0.0"))
channel = connection.channel()
channel.queue_declare(queue="tasks", durable=True)

@app.route("/task",methods=["POST"])
def taskRoute():
    data = request.json
    reply_to = data["replyTo"]

    channel.basic_publish("", "tasks", json.dumps(data), pika.BasicProperties(reply_to=reply_to, delivery_mode=2))

    print("[x] Sent")
    



    res = make_response(jsonify({"status": True}))
    res.headers["Content-Type"] = "application/json"
    return res

app.run("0.0.0.0","5000", debug=True)