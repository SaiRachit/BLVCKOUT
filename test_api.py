import urllib.request
import urllib.error

try:
    urllib.request.urlopen('https://api-service-yq0m.onrender.com/rca/latest')
    print("SUCCESS 200")
except urllib.error.HTTPError as e:
    print(e.read().decode())
