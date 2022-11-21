from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pymongo


class MongoHandler:
    def __init__(self):
        conn_str = "mongodb+srv://eliasmattson:Qtepa112@kafka-mongo.lfvc3bl.mongodb.net/?retryWrites=true&w=majority"
        self.client = pymongo.MongoClient(conn_str)

    def insert_value(self, db, collection, values):
        assert isinstance(values, list), "Values must be in list"
        assert isinstance(values[0], dict), "Items must be dict"
        try:
            self.client[db][collection].insert_many(values)
            print("Item inserted")
        except Exception as e:
            print("Couldn't insert item")
            print(str(e))

    def sell_items(self, db, collection, object_ids, selling_price):
        objects_to_delete = []
        for object_id in object_ids:
            objects_to_delete.append(self.search_collection(db=db,
                                                            collection=collection,
                                                            filter={"_id": object_id}))
        self.delete_values(objects_to_delete)

    def delete_values(self, db, collection, objects):
        assert len(objects) > 0, "Object ids list is empty..."

        try:
            self.client[db][collection].delete_many(objects)
            print(f"Deleted {len(objects)} items.")
        except Exception as e:
            print("Couldn't delete items...")
            print(f"{str(e)}")

    def search_collection(self, db, collection, filter):
        res = self.client[db][collection].find(filter)
        return res

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
