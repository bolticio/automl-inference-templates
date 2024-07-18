import kserve
from typing import Dict
from spello.model import SpellCorrectionModel
from google.cloud import storage
from urllib.parse import urlparse
import pickle
import os

def download_file_from_gcs():
    """Downloads a file from Google Cloud Storage."""
    storage_client = storage.Client()
    parsed_url = urlparse(os.getenv("GCS_STORAGE"))
    bucket_name = parsed_url.netloc
    source_blob_name = parsed_url.path.strip("/")
    destination_file_name=source_blob_name.split("/")[-1]
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"File {source_blob_name} downloaded to {destination_file_name}.")
    return destination_file_name


class SpelloModel(kserve.Model):
    def __init__(self, name: str):
       super().__init__(name)
       self.name = name
       self.load()

    def load(self):
        with open(download_file_from_gcs(), 'rb') as f:
            self.model = pickle.load(f)
        self.ready = True

    def predict(self, payload: Dict, headers: Dict[str, str] = None) -> Dict:
        
        return {
            "predictions": self.model.spell_correct(payload['query'])

        }

if __name__ == "__main__":
    model = SpelloModel(os.getenv("MODEL_NAME"))
    kserve.ModelServer().start([model])