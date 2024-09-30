import requests
import json
import openpyxl
from openpyxl.styles import PatternFill, Alignment, Border, Side
from tqdm import tqdm
from datetime import datetime
import urllib3
from dotenv import load_dotenv
import os
import asyncio
import aiohttp
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
    files = {}
    headers = {
        'Authorization': token
    }
    response = requests.get(url, headers=headers, data=payload, files=files)
    return response.json()

async def get_source_async(response):
    url_list = []
    for item in response['data']['nodes']:
        if item['type'] != "next":
            id = item['id']
            url = "https://edm-delman.apps.binus.edu/analytic/node/" + id + "/sync"
            url_list.append(url)
    return url_list

async def hit_single_api(session, url, token):
    headers = {
        'Content-Type': 'application/json',
        "Authorization": token
    }
    async with session.post(url, headers=headers, json={}) as response:
        status = response.status
        print(f"Sync request to {url} completed with status code: {status}")
        return status

async def hit_api(response, token):
    url_list = await get_source_async(response)

    async with aiohttp.ClientSession() as session:
        tasks = [hit_single_api(session, url, token) for url in url_list]
        
        # Run all tasks concurrently
        await asyncio.gather(*tasks)

    return "All sync requests completed."


if __name__ == '__main__':
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    load_dotenv()
    username = os.environ.get("USERNAME_DELMAN")
    password = os.environ.get("PASSWORD_DELMAN")
    token = get_token(username, password)
    job_id = str(input("Input Job ID Yang Akan Di Sync: "))
    data = get_job(job_id)
    
    # Use asyncio.run() to execute the async function
    asyncio.run(hit_api(data, token))
