import kserve
from typing import Dict
from pymongo import MongoClient
import os

def query_mongodb():
    # Connect to MongoDB
    client = MongoClient(os.getenv("MONGO_CONNECTION_URI"))
    return client

class CustomModel(kserve.Model):
    def __init__(self, name: str):
       super().__init__(name)
       self.name = name
       self.load()
       

    def load(self):
        self.ready = True

    def predict(self, payload: Dict, headers: Dict[str, str] = None) -> Dict:
        print(payload)
        client =query_mongodb()
        # database_name = "fynd"
        database_name = os.getenv("MONGO_DATABASE_NAME")
        # Select the database
        db = client[database_name]

        # Select the collection
        collection_name = os.getenv("MONGO_COLLECTION_NAME")

        print("Collection name : ",collection_name)

        print("Collection list : ",db.list_collection_names())

        if collection_name in db.list_collection_names():
            print(f"The collection '{collection_name}' exists.")
        else:
            print(f"The collection '{collection_name}' does not exist.")
            return {
                "error" : "collection not found"
            }

    
        collection = db[collection_name]

        product_id= payload['product_id'] if "product_id" in payload else None
        product_slug= payload['product_slug'] if "product_slug" in payload else None

        query = {
            "$or": [
                {
                    "product_id": product_id,
                },
                {
                    "product_slug": product_slug,
                }
            ]
        }
        result = list(collection.find(query))
        print(result)
        if len(result)==0:
            return {
                "predictions": []
            }
        
        return {
            "predictions": result[0]["recommendations"]
        }

if __name__ == "__main__":
    model = CustomModel(os.getenv("MODEL_NAME"))
    kserve.ModelServer().start([model])
