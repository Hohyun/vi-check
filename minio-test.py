# file_uploader.py MinIO Python SDK example
from minio import Minio
from minio.error import S3Error

def main():
    client = Minio(
        "10.90.65.61:9000",
        access_key="X6I698S1TZ4N791O9PK2",
        secret_key="nSd6SEEMxVrI5IdD06itUMGt+44StxTiz5i7uVpa",
        secure=False,  # Set to True if using HTTPS
    )

    bucket_name = "datalake"
    object_name = "vi/agent.csv" 
    file_path = "./data/agent.csv"
    client.fget_object(bucket_name, object_name, file_path)

    print(f"File {file_path} downloaded successfully from bucket {bucket_name}.")

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)