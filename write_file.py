from google.cloud import storage

storage_client = storage.Client.from_service_account_json('key.json')
bucket = storage_client.get_bucket("arsen-hw4-bucket")


def upload_from_bucket(destination_blob_name):
    blob = bucket.get_blob('project/' + destination_blob_name)
    return blob.download_as_string().decode('UTF-8')


def upload_to_bucket(destination_blob_name, data):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    blob = bucket.blob('project/' + destination_blob_name)
    blob.upload_from_string(data, content_type="json")
