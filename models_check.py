import requests
import json

api_url = "http://localhost:1337/api/models"
# IMPORTANT: Replace with your actual key
api_key = "sk-API-key"
headers = {"Authorization": f"Bearer sk-API-Key-Here"}

try:
    response = requests.get(api_url, headers=headers, timeout=15)
    response.raise_for_status()
    data = response.json()
    # Pretty print the JSON data
    print(json.dumps(data, indent=2))
except Exception as e:
    print(f"An error occurred: {e}")