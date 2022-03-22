import requests
import json


# SECRET ENV VARIABLES

secret_dict = json.load(open('secret.json', 'r'))

auth_url = secret_dict['auth_url']
post_data_url = secret_dict['post_data_url']
client_secret = secret_dict['client_secret']
client_id = secret_dict['client_id']



# ----------------------------------
# AUTHENTICATE ---------------------
# ----------------------------------



def authenticate(username, password):
    
    data = {
        'grant_type':'password', 
        'client_secret':client_secret, 
        'client_id':client_id, 
        'username':username, 
        'password':password
        }

    r = requests.post(auth_url, data=data)

    access_token = r.json()['access_token']
    refresh_token = r.json()['refresh_token']

    return access_token, refresh_token



# ----------------------------------
# REFRESH AUTHENTICATION -----------
# ----------------------------------
def refresh_authentication(refresh_token):
    data = {
        'grant_type':'refresh_token',
        'client_secret':client_secret,
        'client_id':client_id,
        'refresh_token':refresh_token
    }

    r = requests.post(auth_url, data=data)

    access_token = r.json()['access_token']
    refresh_token = r.json()['refresh_token']

    return access_token, refresh_token

