from flask import Flask, request, jsonify, make_response
from spacy.pipeline import EntityRecognizer
from spacytextblob.spacytextblob import SpacyTextBlob
from flask_cors import CORS
import spacy
import time
import numpy as np

app = Flask(__name__)

cors = CORS()

nlp = spacy.load("en_core_web_trf")
nlp.add_pipe("spacytextblob")



print("Loaded")


@app.route("/tasks/NER", methods=["POST"])
def nerRoute():
    data = request.json["data"]
    print(data)

    out = {}

    for file in data:
        text = file["file"]["text"]
        doc = nlp(text)
        out[file["file"]["id"]] = {"data": [{"text": ent.text, "label": ent.label_} for ent in doc.ents]}
    print(out)
    res = make_response(jsonify(out))
    res.headers["Content-Type"] = "application/json"
    # time.sleep(10)
    return res

@app.route("/tasks/Sentiment", methods=["POST"])
def sentimentRoute():
    files = request.json["data"]
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
    res = make_response(jsonify(out))
    res.headers["Content-Type"] = "application/json"
    print(res)
    return res


def similarity(doc ,other_doc):
    a = np.mean(doc._.trf_data.tensors[1], axis=0)
    b = np.mean(other_doc._.trf_data.tensors[1], axis=0)
    print(a.shape, b.shape)

    res = np.dot(a,b)/ (np.linalg.norm(a) * np.linalg.norm(b))
    print(res)
    assert(res <= 1.0)
    return res.item()

    print("SIMILARITY TENSOR")
    print(doc._.trf_data.tensors[0].shape)
    print(other_doc._.trf_data.tensors[0].shape)

    print(doc._.trf_data.tensors[1].shape)
    print(other_doc._.trf_data.tensors[1].shape)

@app.route("/tasks/Similarity", methods=["POST"])
def similarityRoute():
    files = request.json["data"]
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
    print(out)
    res = make_response(jsonify(out))
    res.headers["Content-Type"] = "application/json"
    return res


        







app.run("localhost","5000", debug=True)