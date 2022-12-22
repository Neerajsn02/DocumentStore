import json
from pymongo import MongoClient

with open('name.basics.json') as a:
    name_basics_data = json.load(a)
with open('title.basics.json') as b:
    title_basics_data = json.load(b)
with open('title.principals.json') as c:
    title_principals_data = json.load(c)
with open('title.ratings.json') as d:
    title_ratings_data = json.load(d)

port = input("Enter port number for server: ")
client = MongoClient(port)

db = client["291db"]


name_basics = db["name_basics"]
title_basics = db["title_basics"]
title_principals = db["title_principals"]
title_ratings = db["title_ratings"]

name_basics.delete_many({})
title_basics.delete_many({})
title_principals.delete_many({})
title_ratings.delete_many({})

name_basics.insert_many(name_basics_data)
title_basics.insert_many(title_basics_data)
title_principals.insert_many(title_principals_data)
title_ratings.insert_many(title_ratings_data)