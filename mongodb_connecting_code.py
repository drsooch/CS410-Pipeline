import pprint
from pymongo import MongoClient
import pymongo

#Testing data 
new_documents = [
  {
    "name": "Sun Bakery Trattoria",
    "stars": 4,
    "categories": ["Pizza","Pasta","Italian","Coffee","Sandwiches"]
  }, {
    "name": "Blue Bagels Grill",
    "stars": 3,
    "categories": ["Bagels","Cookies","Sandwiches"]
  }
]
#pipeline example
pipeline =[

    {"$match": {"categories": "Bakery"}},
    {"$group": {"_id": "$stars", "count": {"$sum": 1}}}
]
class mongodb_connecting_code:
  #creating connection to mongodb
  client = pymongo.MongoClient()


  collection = client.test2.restaurants
  collection.insert_many(new_documents)
  for restaurant in collection.find():
    pprint.pprint(restaurant)
  collection.create_index([('name', pymongo.ASCENDING)])
  print("After indexing,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,")
  for restaurant in collection.find():
    pprint.pprint(restaurant)

  pprint.pprint(list(collection.aggregate(pipeline)))
