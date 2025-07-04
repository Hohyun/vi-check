from minio import Minio, S3Error

def main():
    # Initialize the MinIO client
    
    client = Minio(
        "10.90.65.61",
        access_key="hohyunkim",
        secret_key="CvHacVkcPXfJr0Jh37j8",
        secure=False,  # Set to True if using HTTPS
        region=None
    )

    source_file = "/invoice/CCBLNG_CC_SMRY_250529_250602_250604_T08.27.37.csv.gz"
    bucket_name = "aprs"

    found = client.bucket_exists(bucket_name)
    if not found:
        # client.make_bucket(bucket_name)
        print("Created bucket:", bucket_name)
    else:
        print("Bucket already exists:", bucket_name)
    
    client.fget_object(
        bucket_name,
        "./data/CCBLNG_CC_SMRY_250529_250602_250604_T08.27.37.csv.gz",
        source_file,
    )
    print(
        source_file, "downloaded to ./data/CCBLNG_CC_SMRY_250529_250602_250604_T08.27.37.csv.gz"
    )

if __name__ == "__main__":
    try:
        main() 
    except S3Error as exc:
        print("An error occurred:", exc)

