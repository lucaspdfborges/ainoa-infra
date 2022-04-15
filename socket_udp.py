import datetime
import socket as s
import ast
import json
import os
import numpy as np
from IA.Ainoa import Barragem


from post import post_data, post_state, data_to_post_later, post_late_data, post_late_state, can_post_state, input_state_trim, state_to_post_later
from auth import authenticate, refresh_authentication

# ----------------------------------
# A.I. VARIABLES -------------------
# ----------------------------------

barragem_path = os.path.dirname(os.path.realpath(__file__)) + '/AI'
Paranoa = Barragem(barragem_path)

# ----------------------------------
# JSON VARIABLES -------------------
# ----------------------------------

dadotipo_dict = json.load(open('dadostipos.json', 'r'))
sensores_dict = json.load(open('sensores.json', 'r'))


secret_dict = json.load(open('secret.json', 'r'))
post_data_url = secret_dict['post_data_url']
post_state_url = secret_dict['post_state_url']
state_input_blank = {
    'R8CEBEHE':np.array([]), 'R8CEBEHN':np.array([]), 
    'R8CEBEHZ':np.array([]), 'R016AEHE':np.array([]), 
    'R016AEHN':np.array([]), 'R016AEHZ':np.array([]), 
    'RBAE5EHZ':np.array([]), 'RBAE5ENE':np.array([]), 
    'RBAE5ENN':np.array([]), 'RBAE5ENZ':np.array([]), 
    'RE647EHE':np.array([]), 'RE647EHN':np.array([]), 
    'RE647EHZ':np.array([]), 'RFB89EHE':np.array([]), 
    'RFB89EHZ':np.array([]), 'RFB89EHN':np.array([])
    }
state_input =  state_input_blank.copy()

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

                    # States input for the AI
                    state_key = sensor_name + dadotipo_str
                    state_input[state_key] =  np.append(state_input[state_key], dado)

                    if can_post_state(state_input):
                        state_input = input_state_trim(state_input)
                        Paranoa.atualiza(state_input)
                        state_id = Paranoa.estado + 1 # Database starts w/ 1 not 0, thats why the +1
                        post_state(state_id, timestamp, access_token)
                        state_input =  state_input_blank.copy()

                    # Post last local DB data
                    try:
                        post_late_data(post_data_url, access_token)
                        post_late_state(post_state_url, access_token)
                    except:
                        print('db synced')

                    if post_request.status_code==401:
                        access_token, refresh_token  = authenticate('raspberry','rpshake2021')
                
                    if post_request.status_code!=201:
                        raise Exception('Auth without credentials')

                except:
                    entry = data_to_post_later(sensor_id, dadotipo_id, dado, timestamp)
                    if can_post_state(state_input):
                        state_input = input_state_trim(state_input)
                        Paranoa.atualiza(state_input)
                        state_id = Paranoa.estado + 1 # Database starts w/ 1 not 0, thats why the +1
                        entry_state = state_to_post_later(sensor_id, dadotipo_id, dado, timestamp)
                        state_input =  state_input_blank.copy()

                    print(entry)
                    print(entry_state)

# ----------------------------------
# UDP SOCKET  ----------------------
# ----------------------------------

def socket_udp():
    global access_token, refresh_token
    while True:
        connect_and_post()


if __name__=='__main__':
    socket_udp()
    
