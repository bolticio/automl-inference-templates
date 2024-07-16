import logging
import os
from pathlib import PurePath
from typing import Dict

import kserve
import tensorflow as tf
from keras.models import Model
from kserve import Model
from kserve.model import ModelInferRequest


class DefaultCustomModel(Model):
    def __init__(self, name: str):
        super().__init__(name)
        self.name = name
        self.model = None
        self.bucket_name, self.base_path = self.extract_bucket_and_blob_name(os.getenv("GCS_STORAGE"))
        self.jap_id = os.getenv("jap_id")
        self.recommendation_count = int(os.getenv("recommendation_count", 15))
        self.model_path = os.path.join("gs://", os.fspath(PurePath(self.bucket_name,
                                                                   "personalised_product_recommendations",
                                                                   self.jap_id,
                                                                   "personalised_model.model")))
        self.load()

    def extract_bucket_and_blob_name(self, gcs_uri: str):
        """Extracts the bucket name and blob name from a GCS URI."""
        uri_without_gs = gcs_uri.removeprefix('gs://')
        bucket_name, _, blob_name = uri_without_gs.partition('/')
        return bucket_name, blob_name

    def load(self):
        self.model = tf.saved_model.load(self.model_path)

        logging.info("Model loaded successfully.")
        self.ready = True

    def predict(self, payload: ModelInferRequest, headers: Dict[str, str] = None):
        user_id = payload["user_id"]

        scores, titles = self.model([user_id])
        recommendations = titles[0, :self.recommendation_count]
        extracted_recommendations = []
        for recommendation in recommendations:
            extracted_recommendations.append(int(recommendation.numpy().decode('utf-8')))

        return {"predictions": extracted_recommendations}


if __name__ == "__main__":
    model = DefaultCustomModel(os.getenv("MODEL_NAME"))
    kserve.ModelServer(workers=1).start([model])
