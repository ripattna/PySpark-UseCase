import requests
pload = {'username': 'idap_livenx.gen', 'password': '8USDyZnFxg5ac9cBJUU7NhpU'}
request = requests.get('http://10.34.111.206:8310/api/vpn/current', data=pload)
if request.status_code == 200:
    print("URL is accessible:", request.status_code)
else:
    print("URL is not accessible:", request.status_code)
print(request.json())

'''import requests
req = requests.get("https://www.datacamp.com/")
print(req.status_code)
print(req.headers)
print(req.text)'''

'''
import urllib.request
# open a connection to a URL using urllib
webUrl = urllib.request.urlopen('https://www.youtube.com/user/guru99com')

# Get the result code and print it
print("result code: " + str(webUrl.getcode()))

# read the data from the URL and print it
data = webUrl.read()
print(data)
'''