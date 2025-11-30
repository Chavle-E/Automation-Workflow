from google.cloud import storage
import os
import logging


class CloudStorageDB:
    """Helper to sync SQLite database with Cloud Storage."""

    def __init__(self, bucket_name: str, db_filename: str = "user_mappings.db"):
        """
        Initialize Cloud Storage DB helper.

        Args:
            bucket_name: Your GCS bucket name
            db_filename: Database filename
        """
        self.bucket_name = bucket_name
        self.db_filename = db_filename
        self.local_path = f"/tmp/{db_filename}"
        self.client = storage.Client()

    def download_db(self):
        """Download database from Cloud Storage to /tmp."""
        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(self.db_filename)

            if blob.exists():
                blob.download_to_filename(self.local_path)
                logging.info(f"Downloaded database from gs://{self.bucket_name}/{self.db_filename}")
            else:
                logging.info(f"Database doesn't exist in Cloud Storage yet, will create new one")
        except Exception as e:
            logging.error(f"Error downloading database: {e}")
            # If download fails, we'll just start with a fresh DB

    def upload_db(self):
        """Upload database from /tmp to Cloud Storage."""
        try:
            if not os.path.exists(self.local_path):
                logging.warning("No local database to upload")
                return

            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(self.db_filename)
            blob.upload_from_filename(self.local_path)
            logging.info(f"Uploaded database to gs://{self.bucket_name}/{self.db_filename}")
        except Exception as e:
            logging.error(f"Error uploading database: {e}")

    def get_db_path(self):
        """Get the local path to the database."""
        return self.local_path
