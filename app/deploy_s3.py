
import os
from sys import argv

from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.exceptions import NoCredentialsError

ACCESS_KEY = 'minioaws'
SECRET_KEY = 'minioaws'
host = 'http://35.208.0.141:9000'
bucket_name = 'raw-data'

local_folder, s3_folder = argv[1:3]
walks = os.walk(local_folder)
# Function to upload to s3

def connect():
    session = boto3.session.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )
    connection = session.client('s3',
            endpoint_url=host
    )

    return connection

def upload_to_aws(bucket, local_file, s3_file):
    """local_file, s3_file can be paths"""
    s3 = connect()
    print('  Uploading ' +local_file + ' as ' + bucket + '/' +s3_file)
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print('  '+s3_file + ": Upload Successful")
        print('  ---------')
        return True
    except NoCredentialsError:
        print("Credentials not available")
        return False

def upload_to_aws_parallel(bucket, local_file, s3_file):

    with ThreadPoolExecutor(max_workers=1000) as executor:
        executor.submit(upload_to_aws, bucket, local_file, s3_file)


all_files = []
"""For file names"""
for source, dirs, files in walks:
    print('Directory: ' + source)
    for filename in files:
        # construct the full local path
        local_file = os.path.join(source, filename)
        # construct the full Dropbox path
        relative_path = os.path.relpath(local_file, local_folder)
        s3_file = os.path.join(s3_folder, relative_path)
        # Invoke upload function
        # all_files.append((bucket_name, local_file, s3_file))

        upload_to_aws(bucket_name, local_file, s3_file)

# with ThreadPoolExecutor(max_workers=100) as executor:
#     print('Uploading files to s3')
#     executor.map(lambda x: upload_to_aws(*x), all_files)