import json
import requests
struct = {}

url = 'https://idl-prd-edge5.cisco.com/proxy/v1/feedmgr/feeds'
pload = {'username': 'siadp.gen', 'password': 'Ci$Co#123'}

response = requests.get(url, data=pload, verify=False)

if response.status_code == 200:
    print("URL is accessible:", response.status_code)
else:
    print("URL is not accessible:", response.status_code)

new = response.content.decode('utf-8')
print(new)

'''try:
    # data = r.json()
    print(request.content.decode('utf-8').replace('\0', ''))
except ValueError:
    print("Response content is not valid JSON")'''
