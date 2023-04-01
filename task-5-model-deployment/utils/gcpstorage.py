from sys import prefix
from google.cloud import storage
from google.oauth2 import service_account
from gcpcredits import  gcp_storage_credentials

import datetime
import os
from dotenv import load_dotenv
load_dotenv()

import glob
import logging



class GCPOps():
    #add path to user buket
    def __init__(self,user_name):

        self.credentials = service_account.Credentials.from_service_account_info(gcp_storage_credentials())
        root_project = os.environ.get('GCP_ROOT_PROJECT','')
        self.storage_client = storage.Client(project=root_project,credentials=self.credentials)
        self.user_name = user_name
        self.root_bucket = os.environ.get('GCP_ROOT_BUCKET','')
    
    def generate_download_signed_url_v4(self, blob_name,bucket_name=None):

        bucket = self.storage_client.bucket(self.root_bucket)
        blob = bucket.blob(blob_name)

        url = blob.generate_signed_url(
            version="v4",
            # This URL is valid for 15 minutes
            expiration=datetime.timedelta(minutes=15),
            # Allow GET requests using this URL.
            method="GET",
        )
        return url
    


    def get_bucket(self,bucket_name=None):
        bucket = self.storage_client.lookup_bucket(self.root_bucket)
        return bucket
    
    def check_blob(self,blob_name,bucket_name=None):
        bucket = self.check_bucket(self.root_bucket)
        print(blob_name)
        blob = bucket.list_blobs(prefix =blob_name)
        if blob:
            logging.debug(f'Checking For Folder {bucket_name}')
            return True


    def check_bucket(self,bucket_name=None):
        bucket = self.storage_client.lookup_bucket(self.root_bucket)
        if not bucket:
            return False
        return bucket

    def create_bucket(self,bucket_name=None):
        if not self.check_bucket(self.root_bucket):
            try:
                self.storage_client.create_bucket(self.root_bucket)
                logging.debug(f"Created bucket {self.root_bucket}")
            except Exception as e:
                logging.debug(f"Error creating bucket {self.root_bucket}")
                raise e


    def create_blob(self,blob_name,bucket_name=None):
        bucket = self.storage_client.lookup_bucket(self.root_bucket)
        blob = bucket.blob(blob_name)
        blob.upload_from_string('')
        logging.debug(f"Creating Folder {blob_name} in {self.root_bucket}")
            

    
    def upload_blob(self, source_file_name, destination_blob_name,bucket_name=None):
        """Uploads a file to the bucket."""
        try:
            blob = self.check_blob(self.root_bucket,destination_blob_name)
            if blob:
                blob.upload_from_filename(source_file_name)
                logging.debug(f"File {source_file_name} uploaded to {destination_blob_name}.")
        except Exception as e:
            logging.debug(f"could'nt upload file")
            raise e
        

    def list_blobss(self,project_name,directory_path,bucket_name=None):
        # # Make an authenticated API request
        bucket = self.storage_client.get_bucket(self.root_bucket)
        blobs_list = []
        regex = f"{self.user_name}/{project_name}/{directory_path}/"
        print(regex)
        try:
            for blob in bucket.list_blobs(prefix=self.user_name):
                if (blob.name.startswith(regex)) and not(blob.name.endswith('/')):
                    blobs_list.append(blob.name.split('/')[-1])
                    continue
        except Exception as e:
            logging.debug(f"Couldn't list files from cloud: {e}")
            raise e

        return blobs_list
    
    def download_blob(self, destination_folder,bucket_name=None):
        """Downloads a blob from the bucket."""

        blobs = self.storage_client.list_blobs(self.root_bucket)
        for blob in blobs:
            filename =  blob.name
            destination_file = os.path.join(destination_folder,filename)
            blob.download_to_filename(destination_file)
            file_path = os.path.join(destination_folder,destination_file)
            logging.debug(f"File saved {filename}")
            return file_path
    

    def upload_to_cloud_query(self,query_id,filename,dest_bucket_name=None):
        '''Upload files to cloud and remve from local'''
        # rel_paths = glob.glob(directory_path + '/**', recursive=True)

        bucket = self.storage_client.bucket(self.root_bucket)
        remote_path = f'{self.user_name}/queries/{query_id}/{filename.split(os.sep)[-1]}'
        if os.path.isfile(filename):
            blob = bucket.blob(remote_path)
            blob.upload_from_filename(filename)
            os.remove(filename)

            logging.debug("Uploaded files to cloud")
        else:
            logging.debug(f"Couldn't upload files to cloud:")
            raise "Couldn't upload files"
        
    def upload_to_cloud_reference(self,query_id,filename,dest_bucket_name=None):
        '''Upload files to cloud and remve from local'''
        bucket = self.storage_client.bucket(self.root_bucket)
        remote_path = f'{self.user_name}/queries/{query_id}/{filename.split(os.sep)[-1]}'
        if os.path.isfile(filename):
            blob = bucket.blob(remote_path)
            blob.upload_from_filename(filename)
            os.remove(filename)

            logging.debug("Uploaded files to cloud")
        else:
            logging.debug(f"Couldn't upload files to cloud:")
            raise "Couldn't upload files"
        



# x = GCPOps('sdf')
# print(x.check_blob('atufa/forest-cover-prediction/'))