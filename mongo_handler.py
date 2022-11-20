from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pymongo


class MongoHandler:
    def __init__(self):
        conn_str = "mongodb+srv://eliasmattson:Qtepa112@kafka-mongo.lfvc3bl.mongodb.net/?retryWrites=true&w=majority"
        self.client = pymongo.MongoClient(conn_str)

    def insert_value(self, db, collection, value):
        assert isinstance(value, list), "Values must be in list"
        assert isinstance(value[0], dict), "Items must be dict"
        try:
            self.client[db][collection].insert_many([value])
            print("Item inserted")
        except Exception as e:
            print("Couldn't insert item")
            print(str(e))

    def list_databases(self):
        # Return the names of the databases
        for db in self.client.list_databases():
            print(db)

    def list_collections_in_db(self, db):
        # Return the names of collections in db
        for collection in self.client[db].collection_names():
            print(collection)

    def list_collections(self):
        # Return the names of all collections
        for db in self.client.list_databases():
            for collection in self.client[db.get("name")].collection_names():
                print(collection)

    def delete_database(self, db):
        try:
            self.client[db].drop()
            print(f"dropped db {db}")
        except Exception as e:
            print("Couldnt drop db")
            print(str(e))

    def delete_collection(self, db, collection):
        try:
            self.client[db].drop_collection(collection)
            print(f"Dropped collection {collection}")
        except Exception as e:
            print(f"Dropped collection {collection}")
            print(str(e))
