import os
from typing import Dict

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
        self.client = MongoClient(self.connection_uri)
        self.database = self.client[self.database_name]
        self.collection = self.database[self.collection_name]
        self.ready = True

    def predict(self, payload: Dict, headers: Dict[str, str] = None) -> Dict:

        product_id = payload.get("product_id")
        product_slug = payload.get("product_slug")

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
        result = list(self.collection.find(query))
        if len(result) == 0:
            return {
                "predictions": []
            }

        return {
            "predictions": result[0]["recommendations"]
        }


if __name__ == "__main__":
    model = CustomModel(os.getenv("MODEL_NAME"))
    kserve.ModelServer().start([model])
