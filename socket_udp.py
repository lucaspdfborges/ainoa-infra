import datetime
import socket as s
import ast
import json
import numpy as np

from post import post_data, data_to_post_later, post_late_data
from auth import authenticate, refresh_authentication

# ----------------------------------
# JSON VARIABLES --------------------
# ----------------------------------

dadotipo_dict = json.load(open('dadostipos.json', 'r'))
sensores_dict = json.load(open('sensores.json', 'r'))

secret_dict = json.load(open('secret.json', 'r'))
post_data_url = secret_dict['post_data_url']

# ----------------------------------
# AUTH VARIABLES --------------------
# ----------------------------------
access_token = ''
refresh_token = ''

# --------------------------------------
# Connect and Post to Server / Database
# --------------------------------------

def connect_and_post():

    global access_token, refresh_token

    for sensor_name in sensores_dict:

        sensor = sensores_dict[sensor_name]
        
        host = ""           # when running not on the Shake Pi: blank = localhost 
        port = sensor['port']        # Port to bind to
        sensor_id = sensor['id']    

        sock = s.socket(s.AF_INET, s.SOCK_DGRAM)
        sock.bind((host, port))

        while True:
                data, addr = sock.recvfrom(1024)   
                dict_str = data.decode("UTF-8")    

                dadotipo_str = dict_str[2:5]
                dadotipo_id = dadotipo_dict[dadotipo_str]
                
                unixtime_str = dict_str[8:22]
                timestamp =  datetime.datetime.utcfromtimestamp(float(unixtime_str)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                
                dados_str = "{"+dict_str[24:len(dict_str)]
                dados = ast.literal_eval(dados_str)

                for dado in dados:
                    try:
                        # Post Current UDP data
                        post_request = post_data(sensor_id, dadotipo_id, dado, timestamp, access_token)
                        print(post_request.status_code, post_request.json())

                        # Post last local DB data
                        try:
                            post_late_data(post_data_url, access_token)
                        except:
                            print('db synced')


                        if post_request.status_code==401:
                            access_token, refresh_token  = authenticate('raspberry','rpshake2021')
                    
                        if post_request.status_code!=201:
                            raise Exception('Auth without credentials')

                    except:
                        entry = data_to_post_later(sensor_id, dadotipo_id, dado, timestamp)
                        print(entry)

                    
                
# ----------------------------------
# UDP SOCKET  ----------------------
# ----------------------------------

def socket_udp():

    global access_token, refresh_token
    
    while True:
        connect_and_post()


if __name__=='__main__':
    socket_udp()
    
