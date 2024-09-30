import requests
import json
import openpyxl
from openpyxl.styles import PatternFill, Alignment, Border, Side
from tqdm import tqdm
from datetime import datetime
import urllib3
from dotenv import load_dotenv
import os
from collections import defaultdict
import sys


def get_token(username, password):
    login_path = "https://edm-delman.apps.binus.edu/analytic/login"

    payload = json.dumps({
        "username": username,
        "password": password
    })
    headers = {
    'Content-Type': 'application/json'
    }

    token = requests.post(
        login_path, 
        headers=headers, 
        data=payload, 
        verify=False
    ).headers["Authorization"]
    
    return token


def get_job(job_id):

    
    url = "https://edm-delman.apps.binus.edu/analytic/projects/"+job_id
    payload = {}
    files={}
    headers = {
    'Authorization': token
    }

    response = requests.get(url, headers=headers, data=payload, files=files)
    return response.json()

def get_source(response):
    # Access the 'nodes' key in the dictionary
    url_list = []
    for item in response['data']['nodes']:
        if item['type'] != "next":
            # Access 'name' and 'type' keys in each item
            id = item['id']
            url = "https://edm-delman.apps.binus.edu/analytic/node/"+id+"/sync"
            url_list.append(url)
            print(url)
    return url_list

def hit_api(response,token): 
    url_list = get_source(response)
    for url in url_list:
        payload = json.dumps({})
        headers = {
        'Content-Type': 'application/json',
        "Authorization":token
        }
        response = requests.request("POST", url, headers=headers, data=payload)
    return response



if __name__ == '__main__':
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    load_dotenv()
    username = os.environ.get("USERNAME_DELMAN")
    password = os.environ.get("PASSWORD_DELMAN")
    token = get_token(username, password)
    job_id = str(input("Input Job ID Yang Akan Di Sync: "))
    data = get_job(job_id)
    hit_api(data,token)