from post import post_late_data
import json


secret_dict = json.load(open('secret.json', 'r'))
post_data_url = secret_dict['post_data_url']


def sync_database():
    while True:
        post_late_data(post_data_url, access_token)


if __name__=='__main__':
    sync_database()