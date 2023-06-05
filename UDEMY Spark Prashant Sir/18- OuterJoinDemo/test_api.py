import requests

response = requests.get("http://127.0.0.1:5000/users/getall")
# print(response.text)

data = {
    "id" : 1,
    "emp_name" : "Saurabh Ghosh",
    "emp_email" : "saurabh.ghosh@gmail.com",
    "avatar" : "avatar1.png"
}

url = "http://127.0.0.1:5000/"
post_data = requests.post()