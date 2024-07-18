import os
import kserve
from typing import Dict, Any
from pymongo import MongoClient
from bson import ObjectId

def query_mongodb():
    client = MongoClient(os.getenv("MONGO_CONNECTION_URI"))
    return client

class CustomModel(kserve.Model):
    def __init__(self, name: str):
        super().__init__(name)
        self.name = name
        self.database_name = os.getenv("MONGO_DATABASE_NAME")
        self.collection_name = os.getenv("MONGO_COLLECTION_NAME")
        self.load()
    
    def load(self):
        self.ready = True

    def query_collection(self, collection_name: str, query: Dict[str, Any]) -> Dict[str, Any]:
        client = query_mongodb()
        db = client[self.database_name]

        if collection_name not in db.list_collection_names():
            print(f"The collection '{collection_name}' does not exist.")
            return {"error": "collection not found"}

        collection = db[collection_name]
        result = list(collection.find(query))
        
        for doc in result:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        if len(result) == 0:
            return {"predictions": []}

        return {"predictions": result[0]["recommendations"]}

    def predict(self, payload: Dict[str, Any], headers: Dict[str, str] = None) -> Dict[str, Any]:
        query = {}
        if payload is not None:
            product_slug = payload.get('product_slug', [])
            if isinstance(product_slug, list) and len(product_slug) > 0: 
                query["product_slug"] = { "$in": product_slug }
            elif isinstance(product_slug, str) and product_slug != None: 
                query["product_slug"] = product_slug 
            else:
                return {"error": " Please provide atleast one product_slug"}

        print("Final query: ", query)

        return self.query_collection(self.collection_name, query)

if __name__ == "__main__":
    model = CustomModel(os.getenv("MODEL_NAME"))
    kserve.ModelServer().start([model])
