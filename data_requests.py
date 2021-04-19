import requests
import json
class data_requests:


    def response():
        response = requests.get(url="https://www.courtlistener.com/api/rest/v3/opinions/4582560")
        response.raise_for_status()
        data=response.json()
        return data

#saving data into a variable 
bulk_data = data_requests.response()
print(bulk_data)
#use .json extension instead of .txt to get json file 
with open("data_from_court_listener.txt", mode="w") as file:
    json.dump( bulk_data, file)