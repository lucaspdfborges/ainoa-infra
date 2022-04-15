import requests
import sqlite3
import json

"""
 CREATE TABLE dados( 
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    SENSOR_ID      INT     NOT NULL,
    DADOTIPO_ID    INT     NOT NULL,
    VALOR          INT     NOT NULL,  
    TIMESTAMP      TEXT    NOT NULL
);
"""

""""
INSERT INTO dados (SENSOR_ID,DADOTIPO_ID,VALOR,TIMESTAMP) VALUES (4,1,1100,"2019-08-24T16:05:22Z");
"""

secret_dict = json.load(open('secret.json', 'r'))
post_data_url = secret_dict['post_data_url']
post_state_url = secret_dict['post_state_url']

def data_to_post_later(sensor_id, dadotipo_id, value, timestamp):

    conn = sqlite3.connect('rpi.db')
    c = conn.cursor()
    query = 'INSERT INTO dados (SENSOR_ID,DADOTIPO_ID,VALOR,TIMESTAMP) VALUES (%d,%d,%d,"%s");'%(sensor_id, dadotipo_id,value,timestamp)
    c.execute(query)
    conn.commit()
    c.close()

    return query

def state_to_post_later(state_id, timestamp):

    conn = sqlite3.connect('rpi.db')
    c = conn.cursor()
    query = 'INSERT INTO dados (STATE_ID,TIMESTAMP) VALUES (%d,"%s");'%(state_id, timestamp)
    c.execute(query)
    conn.commit()
    c.close()

    return query

def post_late_data(post_data_url, access_token):

    # Open connection and read data
    conn = sqlite3.connect('rpi.db')
    c = conn.cursor()
    c.execute('SELECT * FROM dados ORDER BY ID ASC LIMIT 1')
    fetched_data = c.fetchall()[0]
    
    data = {
        "sensor": fetched_data[1],
        "tipo": fetched_data[2],
        "value": fetched_data[3],
        "timestamp": fetched_data[4],
    }

    print(" # Late DB: ",data)

    # Delete data and close connection
    c.execute('DELETE FROM dados WHERE id = '+str(fetched_data[0]))
    conn.commit()
    c.close()

    # Post data to API / Database
    bearer_auth = 'Bearer '+ str(access_token)
    headers = { 'Authorization': bearer_auth }
    r = requests.post(post_data_url, data=data, headers=headers)

    return r


def post_late_state(post_state_url, access_token):

    # Open connection and read data
    conn = sqlite3.connect('rpi.db')
    c = conn.cursor()
    c.execute('SELECT * FROM estados ORDER BY ID ASC LIMIT 1')
    fetched_data = c.fetchall()[0]
    
    data = {
        "estado": fetched_data[1],
        "timestamp": fetched_data[2],
    }

    print(" # Late DB: ",data)

    # Delete data and close connection
    c.execute('DELETE FROM estados WHERE id = '+str(fetched_data[0]))
    conn.commit()
    c.close()

    # Post data to API / Database
    bearer_auth = 'Bearer '+ str(access_token)
    headers = { 'Authorization': bearer_auth }
    r = requests.post(post_state_url, data=data, headers=headers)

    return r

# Check if arrays are complete to post state

def can_post_state(state_input):
    for key in state_input:
        if len(state_input[key])<100:
            return False
    return True

def input_state_trim(state_input):
    for key in state_input:
        state_input[key] = state_input[key][len(state_input[key])-100:len(state_input[key])]
    return state_input 

# post state when online
def post_state(state_id, timestamp, access_token):

    data = {
        "estado": state_id,
        "timestamp": timestamp,
    }

    bearer_auth = 'Bearer '+ str(access_token)
    headers = { 'Authorization': bearer_auth }

    r = requests.post(post_state_url, data=data, headers=headers)

    return r

# Post data when Online 
def post_data(sensor_id, dadotipo_id, value, timestamp, access_token):

    data = {
        "sensor": sensor_id,
        "tipo": dadotipo_id,
        "value": value,
        "timestamp": timestamp #"2019-08-24T14:15:22Z"
    }

    bearer_auth = 'Bearer '+ str(access_token)
    headers = { 'Authorization': bearer_auth }

    r = requests.post(post_data_url, data=data, headers=headers)

    return r



