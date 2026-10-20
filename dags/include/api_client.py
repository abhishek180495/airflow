import requests
import json

# url = "http://localhost:5000/loginUsers"

# payload = json.dumps({
#   "start_date": "2026-06-10",
#   "end_date": "2026-06-10",
#   "limit": 50
# })
# headers = {
#   'accept': 'application/json',
#   'Content-Type': 'application/json',
#   'Authorization': 'Basic YWRtaW46YWJoaXNoZWs='
# }

# response = requests.request("POST", url, headers=headers, data=payload)

# print(json.dumps(response.json(), indent=4))

def call_api(url, start_date, end_date, auth_header):
    payload={
        "start_date": start_date,
        "end_date": end_date
    }
    headers={
        "Content-Ty"
    }