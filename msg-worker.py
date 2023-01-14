import json
import pika, sys,os
import spacy
from spacytextblob.spacytextblob import SpacyTextBlob
import requests
import numpy as np
from dotenv import load_dotenv
load_dotenv()
NEXT_URL = os.environ.get("NEXT_URL")
print("nexty", NEXT_URL)

nlp = spacy.load("en_core_web_trf")
nlp.add_pipe("spacytextblob")
def ner_task(data: list):
    out = {} 
    for file in data:
                text = file["file"]["text"]
                doc = nlp(text)
                out[file["file"]["id"]] = {"data": [{"text": ent.text, "label": ent.label_} for ent in doc.ents]}
    return out
def sentiment_task(data: list):
    files = data
    print("FILES", files)
    ids = [file["fileID"] for file in files]
    docs = [nlp(file["file"]["text"]) for file in files]

    out = {}
    for ix, doc in enumerate(docs):
        out[ids[ix]] = {
            "score": (sent_score:=doc._.blob.polarity),
            "sentiment": "POS" if sent_score > 0.0 else  "NEU" if sent_score == 0 else  "NEG",
            "pos_words": [each[0][0] for each in doc._.blob.sentiment_assessments.assessments if each[1] > 0],
            "neg_words": [each[0][0] for each in doc._.blob.sentiment_assessments.assessments if each[1] < 0]
            }
    return out 

def similarity(doc ,other_doc):
    a = np.mean(doc._.trf_data.tensors[1], axis=0)
    b = np.mean(other_doc._.trf_data.tensors[1], axis=0)
    print(a.shape, b.shape)

    res = np.dot(a,b)/ (np.linalg.norm(a) * np.linalg.norm(b))
    print(res)
    assert(res <= 1.0)
    return res.item()
def similarity_task(data:list):
    files = data
    print("FILES",files)
    ids = [file["fileID"] for file in files]
    names = {file["fileID"]: file["file"]["name"] for file in files}
    docs = [nlp(file["file"]["text"]) for file in files]


    out = {}
    for ix, doc in enumerate(docs):
        similarities = {
            "name": names[ids[ix]],
            "similarities": {}
        }
        most_sim = None
        most_sim_key = None
        least_sim = None
        least_sim_key = None
        for idx, other_doc in enumerate(docs):
            if idx == ix:
                continue
            # similarity(doc, other_doc)
            sim_1 = similarity(doc, other_doc)
            sim = sim_1
            if most_sim is None or sim > most_sim :
                most_sim = sim
                most_sim_key = ids[idx]
            
            if least_sim is None or sim < least_sim:
                least_sim = sim
                least_sim_key = ids[idx]


            if other_doc == doc:
                continue
            if other_doc in out:
                similarities["similarities"][ids[idx]] = {
                    "score": out[ids[idx]]["similarities"][ids[ix]],
                    "name": names[ids[idx]]
                    }
            
            else:
                similarities["similarities"][ids[idx]] = {
                    "score":sim,
                    "name": names[ids[idx]]
                    }
            similarities["mostSimilar"] = {
                "id": most_sim_key,
                "name": names[most_sim_key]
                }
            similarities["leastSimilar"] = {
                "id": least_sim_key,
                "name": names[least_sim_key]
                }



        out[ids[ix]] = similarities
    
    return out



    

def main():



    print("Loaded")

    connection = pika.BlockingConnection(pika.ConnectionParameters("0.0.0.0"))
    channel = connection.channel()
    channel.queue_declare(queue="tasks", durable=True)

    def task_callback(ch,method,properties,body):
        print(properties.reply_to)
        # print(f"RECEIVED: {body}")
        payload = json.loads(body.decode("utf-8"))
        data = payload["data"]
        taskID = payload["task"]
        taskType = payload["taskType"]
        print("TASK", taskType)


        if taskType == "NER":
            out = ner_task(data)
        elif taskType == "Sentiment":
            out = sentiment_task(data)
        elif taskType == "Similarity":
            out = similarity_task(data)

        
        # print(out)
        ch.queue_declare(queue=properties.reply_to, durable=True)
        out = json.dumps({"taskID": taskID, "data": out})
        try:
            stat = requests.post(f"{NEXT_URL}", out)
            print(stat.status_code)
        except requests.ConnectionError as  e:
            print(e)

        ch.basic_publish(exchange="amq.topic", routing_key=properties.reply_to, body=json.dumps({"status": True}))
        print("[x] Sent to reply")
        ch.basic_ack(delivery_tag=method.delivery_tag)


    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="tasks", on_message_callback=task_callback)

    print("[*] Waiting for msgs. CTRL+C to exit")
    channel.start_consuming()



if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("INTERRUPTED")
        try: 
            sys.exit(0)
        except SystemExit:
            os._exit(0)