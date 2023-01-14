import requests 
import json
# test hitting next api from external service
cos = requests.post("http://localhost:3000/api/postdata", json.dumps({"yaya":90}))
print(cos.status_code)