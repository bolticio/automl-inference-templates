import logging
import os
import time
from typing import Any, Dict

import kserve
from pymongo import MongoClient


class CustomModel(kserve.Model):
    def __init__(self, name: str):
        super().__init__(name)
        self.name = name
        self.connection_uri = os.getenv("MONGO_CONNECTION_URI")
        self.database_name = os.getenv("MONGO_DATABASE_NAME")
        self.collection_name = os.getenv("MONGO_COLLECTION_NAME")
        self.load()

    def load(self):
        max_retries = 3
        retry_delay = 5  # seconds
        retries = 0
        connected = False
        while retries < max_retries and not connected:
            try:
                self.client = MongoClient(self.connection_uri)
                self.database = self.client[self.database_name]
                self.collection = self.database[self.collection_name]
                connected = True
            except Exception as e:
                logging.error(f"Failed to connect to MongoDB: {e}. Retrying...")
                retries += 1
                if retries < max_retries:
                    time.sleep(retry_delay)
                else:
                    raise e
        self.ready = True if connected else False

    def predict(self, payload: Dict[str, Any], headers: Dict[str, str] = None) -> Dict[str, Any]:
        product_slug = payload.get("product_slug", [])
        if len(product_slug) == 0:
            return {"error": "No Product slug found"}

        # Step 1: Get categories of items in the cart
        cart_categories_cursor = self.collection.find(
            {"product_slug": {"$in": product_slug}},
            {"category": 1, "_id": 0}
        )

        cart_categories = [doc["category"].lower() for doc in cart_categories_cursor if "category" in doc]

        # Step 2: Use aggregation pipeline to exclude recommendations with those categories
        aggregation_pipeline = [
            {
                "$match": {
                    "product_slug": {"$in": product_slug},
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
                                        {"$toLower": "$$recommendation.category"},
                                        cart_categories,
                                    ],
                                },
                            },
                        },
                    },
                },
            },
        ]

        result_cursor = self.collection.aggregate(aggregation_pipeline)
        result = list(result_cursor)

        # Transform _id to string for JSON compatibility
        for doc in result:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        if len(result) == 0:
            return {"predictions": []}

        return {"predictions": result[0]["recommendations"]}


if __name__ == "__main__":
    model = CustomModel(os.getenv("MODEL_NAME"))
    kserve.ModelServer().start([model])
