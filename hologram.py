import requests
import base64
import json
import streamlit as st
import sys
import pandas as pd

ORG_ID = st.secrets['hologram_ORG_ID']
API_KEY = st.secrets['hologram_API_KEY']
API_KEY_ENC = base64.b64encode(f'apikey:{API_KEY}'.encode('utf-8')).decode('utf-8')


@st.cache_data(ttl = 5*60)
def api_request(url):
    headers = {
        'Authorization': f'Basic {API_KEY_ENC}'
    }
    response = requests.get(url, headers = headers)
    print('api_request', url)
    return response.json()


def print_json(j):
    r = json.dumps(j, indent = 2)
    print(r)
    return r


@st.cache_data(ttl = 60*60)
def get_all_links():
    result = list()
    continues = True
    url = f'https://dashboard.hologram.io/api/1/links/cellular'
    while continues:
        links = api_request(url)
        print_json(links)
        if 'data' not in links:
            break
        result.extend(links['data'])
        if 'continues' not in links:
            break
        continues = links['continues']
        if 'links' not in links or 'next' not in links['links']:
            break
        url = f'https://dashboard.hologram.io{links['links']['next']}'

    print(f'get_all_links found {len(result)}')
    return result


def get_link_by_name(name):
    df = pd.DataFrame(get_all_links())
    matches = df[df['devicename'].str.contains(name)]
    if len(matches.index) == 1:
        result = matches.to_dict(orient = 'records')[0]
        print(f'get_link_by_name name={name} {result}')
        return result

    if len(matches.index) > 1:
        print(f'get_link_by_name multiple matches')
        print(matches)
        return matches.to_dict(orient = 'records')[0]

    print(f'get_link_by_name name={name} no matches found')
    print(df.to_dict(orient = 'records'))
    return None


def link_last_location(name = None, deviceid = None):
    if name:
        link = get_link_by_name(name)
        print(f'link_last_location name={name} {link}')
        if not link:
            print(f'link_last_location could not find link with name={name}')
        deviceid = link['deviceid']

    if not name and not deviceid:
        return None

    # resp contains [data] [lastsession] longitude, latitude
    resp = api_request(
        f'https://dashboard.hologram.io/api/1/devices/{deviceid}')
    if 'data' not in resp:
        raise KeyError(f'resp missing data deviceid={deviceid} \n {print_json(resp)}')
    if 'lastsession' not in resp['data']:
        print(f'link_last_location deviceid={deviceid} missing lastsession')
        return None

    return {
        'longitude': resp['data']['lastsession']['longitude'],
        'latitude': resp['data']['lastsession']['latitude']
    }


if __name__ == '__main__':
    link = get_link_by_name('52798')
    print(link)
    print(link_last_location(name = '52798'))

    sys.exit()

    links = api_request(f'https://dashboard.hologram.io/api/1/links/cellular')
    for link in links['data']:
        print(link_last_location(link['deviceid']))

        # NUMBER_OF_SESSIONS_TO_RETURN = 3
        # print_json(api_request(f'https://dashboard.hologram.io/api/1/usage/data?limit={NUMBER_OF_SESSIONS_TO_RETURN}&linkid={link['id']}'))
    # print_json(api_request(f'https://dashboard.hologram.io/api/1/plans?orgid={ORG_ID}'))