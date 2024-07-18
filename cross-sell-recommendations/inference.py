import os
import kserve
from typing import Dict, Any
from pymongo import MongoClient

def query_mongodb():
    client = MongoClient(os.getenv("MONGO_CONNECTION_URI"))
    return client

#Cross sell model
class CustomModel(kserve.Model):
    def __init__(self, name: str):
        super().__init__(name)
        self.name = name
        self.database_name = os.getenv("MONGO_DATABASE_NAME")
        self.collection_name = os.getenv("MONGO_COLLECTION_NAME")
        self.load()
    
    def load(self):
        self.ready = True

    def query_collection(self, collection_name: str, product_slug: list) -> Dict[str, Any]:
        client = query_mongodb()
        db = client[self.database_name]

        if collection_name not in db.list_collection_names():
            print(f"The collection '{collection_name}' does not exist.")
            return {"error": "collection not found"}

        collection = db[collection_name]

        # Step 1: Get categories of items in the cart
        cart_categories_cursor = collection.find(
            { "product_slug": { "$in": product_slug } },
            { "category": 1, "_id": 0 }
        )
        
        cart_categories = [doc["category"].lower() for doc in cart_categories_cursor if "category" in doc]

        # Step 2: Use aggregation pipeline to exclude recommendations with those categories
        aggregation_pipeline = [
            {
                "$match": {
                    "product_slug": { "$in": product_slug },
                },
            },
            {
                "$project": {
                    "product_slug": 1,
                    "category": 1,
                    "recommendations": {
                        "$filter": {
                            "input": "$recommendations",
                            "as": "recommendation",
                            "cond": {
                                "$not": {
                                    "$in": [
                                        { "$toLower": "$$recommendation.category" },
                                        cart_categories,
                                    ],
                                },
                            },
                        },
                    },
                },
            },
        ]

        result_cursor = collection.aggregate(aggregation_pipeline)
        result = list(result_cursor)

        # Transform _id to string for JSON compatibility
        for doc in result:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        if len(result) == 0:
            return {"predictions": []}

        return {"predictions": result[0]["recommendations"]}

    def predict(self, payload: Dict[str, Any], headers: Dict[str, str] = None) -> Dict[str, Any]:
        product_slug = payload.get("product_slug", [])
        if isinstance(product_slug, list) and len(product_slug) > 0: 
            return self.query_collection(self.collection_name, product_slug)
        
        return {"error": "No Product slug found"}

       

if __name__ == "__main__":
    model = CustomModel(os.getenv("MODEL_NAME"))
    kserve.ModelServer().start([model])
