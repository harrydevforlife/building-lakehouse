import json
import requests


def request_recommend(data: dict) -> dict:

    url = 'http://35.208.0.141:5001/api'

    j_data = json.dumps(data)

    headers = {
        'content-type': 'application/json', 
        'Accept-Charset': 'UTF-8'
        }
    
    response = requests.post(url, data=j_data, headers=headers)
    if response.status_code != 200:
        return (f"Error: {response.status_code}")
    return response.json()