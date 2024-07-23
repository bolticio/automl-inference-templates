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

        if len(result)==0:
            return {
                "predictions": []
            }
    
        return {"predictions": result[0]["recommendations"]}

    def predict(self, payload: Dict[str, Any], headers: Dict[str, str] = None) -> Dict[str, Any]:
        query = {}
        if payload is not None:
            key = payload.get('key')
            product_id = payload.get('product_id')
            product_slug = payload.get('product_slug')
            brand = payload.get('brand')
            gender = payload.get('gender')
            category = payload.get('category')
            validKeys = [
                "global",
                "global_brand",
                "global_category",
                "trending_brand",
                "trending_brand_gender",
                "trending_category",
                "trending_category_gender"
            ]
            if key not in validKeys:
                return {"error": "No key found"}

            query["key"] = key

            if product_id is not None:
                query["product_id"] = product_id
            if product_slug is not None:
                query["product_slug"] = product_slug
            if brand is not None:
                query["brand"] = brand
            if gender is not None:
                query["gender"] = gender
            if category is not None:
                query["$or"] = [{ "category": category},{ "category_name": category}]

        print("Final query: ", query)

        return self.query_collection(self.collection_name, query)

if __name__ == "__main__":
    model = CustomModel(os.getenv("MODEL_NAME"))
    kserve.ModelServer().start([model])
