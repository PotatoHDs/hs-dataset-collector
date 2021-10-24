import json

with open('api_response.json') as file:
    data = json.loads(file.read())['data']
print(data)
print(len(data))
