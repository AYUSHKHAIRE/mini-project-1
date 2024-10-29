from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

class AtlasClient:
    def __init__(self, atlas_uri, dbname):
        self.mongodb_client = MongoClient(atlas_uri)
        self.database = self.mongodb_client[dbname]

    def ping(self):
        try:
            self.mongodb_client.admin.command('ping')
            print("Pinged your MongoDB deployment. Connection successful.")
        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}")

    def get_collection(self, collection_name):
        collection = self.database[collection_name]
        return collection

    def find(self, collection_name, filter={}, limit=0):
        collection = self.database[collection_name]
        items = list(collection.find(filter=filter, limit=limit))
        return items
    
    def insert(self, collection_name, documents):
        """
        Inserts one or more documents into a MongoDB collection.
        
        Parameters:
        - collection_name: str, the name of the collection
        - documents: dict or list of dicts, the document(s) to insert
        
        If `documents` is a list, it will insert multiple documents using `insert_many`.
        Otherwise, it will insert a single document using `insert_one`.
        """
        collection = self.get_collection(collection_name)
        
        if isinstance(documents, list):
            result = collection.insert_many(documents)
            print(f"Inserted {len(result.inserted_ids)} documents.")
            return result.inserted_ids
        else:
            result = collection.insert_one(documents)
            print(f"Inserted one document with ID: {result.inserted_id}")
            return result.inserted_id
        
    def delete(self, collection_name, filter={}, _del_all_=False):
        """
        Deletes documents from a MongoDB collection based on the filter.
        
        Parameters:
        - collection_name: str, the name of the collection.
        - filter: dict, the filter to find documents to delete (default is {}).
        - _del_all_: bool, if True, deletes all documents matching the filter using `delete_many()`.
                      If False, deletes only one document using `delete_one()`.
        
        Returns:
        - Number of documents deleted.
        """
        collection = self.get_collection(collection_name)
        
        if _del_all_:
            result = collection.delete_many(filter)
            print(f"Deleted {result.deleted_count} documents.")
            return result.deleted_count
        else:
            result = collection.delete_one(filter)
            if result.deleted_count == 1:
                print("Deleted one document.")
            else:
                print("No document found to delete.")
            return result.deleted_count
    
