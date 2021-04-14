# Useful link https://realpython.com/python-requests/
#
# Makes a simple request to the endpoint provided. Currently errors are printed
# out, and valid requests return the json.
#
# todo: store results into a mongoDB, switch to logging?
#

import requests

def extract(endpoint):

    try:
        response = requests.get(endpoint)
        #when adding in authentication, it may look something like this:
        # response = requests.get(endpoint, auth=('username', getpass())

    except Exception as error:
        print(f'An exception has occurred. : Not able to connect to {endpoint}\n{error}')
        return


    #after successful API call, we can process the data
    if response.status_code == 200:
        return response.json()['results']
    else:
        print(f'Error Acessing {endpoint}.')
        print(response)


#
#Tests
#

#invalid website 
print("trying a website that doesn't exist: ")
print(str(extract("https://www.courtzistener.com/api/rest/v3/"))+"\n")

#valid website, but not a valid endpoint
print("trying an invalid endpoint: ")
print(str(extract("https://www.courtlistener.com/api/rest/v3/pizza-party/")) + "\n")

#A valid website, and a valid endpoint
#should return json
print("trying a real courtlistener endpoint: ")
print(extract("https://www.courtlistener.com/api/rest/v3/dockets/?court__jurisdiction=F&court__jurisdiction=FD"))