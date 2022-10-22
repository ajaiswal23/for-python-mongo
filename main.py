import os
from re import L, M
from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient

load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://addy:{password}@cluster0.rat5bwl.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(connection_string)

dbs = client.list_database_names()
# print(dbs)

test_db = client.test
# t_db = client["test"]

# print(t_db.list_collection_names())
collections = test_db.list_collection_names()
# print(collections)

def insert_test_collection_doc():
    collection = test_db.test_collection

    test_document = {
        "name": "Aditya",
        "type": "Test"
    }

    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)

# insert_test_collection_doc()

production = client.production
person_collection = production.person_collection

def create_documents():
    first_names = ['a','b','c','d','e','f','g','h','i']
    last_names = ['j','k','l','m','n','o','p','q','r']
    ages = [1, 2, 3, 4, 5, 6, 7, 8, 9]

    docs = []
    for first_name, last_name, age in zip(first_names, last_names, ages):
        docs.append({"first_name": first_name, "last_name": last_name, "age": age})

    person_collection.insert_many(docs)

# create_documents()
printer = pprint.PrettyPrinter()
def query_database():
    people = person_collection.find()
    # print(list(people))


    for person in people:

        printer.pprint(person)

# query_database()



############# Get Queries ###########


def find_a():
    people = person_collection.find_one({"first_name": "a"})
    printer.pprint(people)

# find_a()

def count_all_people():
    count_all = person_collection.count_documents(filter={})
    # find().count got deprecated
    #count_all_other = person_collection.find().count()
    print("Number of people: ", count_all)
    

# count_all_people()

def find_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)

# find_by_id("634faaa692b966441b28afaf")

def get_age_range(min_age, max_age):
    query = {"$and": [
                {"age": {"$gte": min_age}},
                {"age": {"$lte": max_age}}
        ]}

    people = person_collection.find(query).sort("age")

    for person in people:
        printer.pprint(person)

# get_age_range(2, 6)

def project_columns():
    columns = {"_id": 0, "first_name": 1, "last_name": 1}
    people = person_collection.find({}, columns)

    for person in people:
        printer.pprint(person)
# project_columns()


############ update query ###############

def update_person_id(person_id):

    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    # all_updates = {
    #     "$set": {"new_field": True},
    #     "$inc": {"age": 1},
    #     "$rename": {"first": "first_name", "last": "last_name"}

    #     }
    # person_collection.update_one({"_id": _id}, all_updates)
    
    # unset means delete
    # person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})

# update_person_id("634faaa692b966441b28afaf")

def replace_one(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    # we'll have a document and you want to replace a document witj
    # you do this when you are updating all of their contact information
    # but they want to keep their same id

    new_doc = {
        "first_name": "Aditya",
        "last_name": "Jaiswal",
        "age": 27
    }

    person_collection.replace_one({"_id": _id}, new_doc)
    # replace work like set in sql
    # here i'm doing it by using id
# replace_one("634faaa692b966441b28afb0")


############ Delete Queries #############

def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    person_collection.delete_one({"_id": _id})

    # person_collection.delete_many({enter yout query here})

# delete_doc_by_id("634faaa692b966441b28afb6")

############## RelationShips in NoSql Database############
#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