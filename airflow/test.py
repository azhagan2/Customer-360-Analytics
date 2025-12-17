import requests


API_URL = "http://104.237.2.219:9050/generate-shipments?100"
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" 
headers = {"Authorization": f"Bearer {TOKEN}"}
response = requests.get(API_URL, headers=headers)
response.raise_for_status()
print(response.json())