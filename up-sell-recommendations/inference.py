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
        query = {}
        if payload is not None:
            product_slug = payload.get("product_slug", [])
            if isinstance(product_slug, list) and len(product_slug) > 0:
                query["product_slug"] = {"$in": product_slug}
            elif isinstance(product_slug, str) and product_slug != None:
                query["product_slug"] = product_slug
            else:
                return {"error": " Please provide atleast one product_slug"}

        result = list(self.collection.find(query))

        for doc in result:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        if len(result) == 0:
            return {"predictions": []}

        return {"predictions": result[0]["recommendations"]}


if __name__ == "__main__":
    model = CustomModel(os.getenv("MODEL_NAME"))
    kserve.ModelServer().start([model])
