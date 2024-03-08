import boto3 # for connecting to FlashBlade using Boto3
import json
import pandas as pd
import os
import time # For looping script every 15 secs
import FinnhubCassandraConversion as converter
import CassandraLoader as loader
import uuid # for generating unique ID for each trade
import logging

# Configure Logger
logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s:%(funcName)s()] %(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)

#FB access
fb_access_key = os.environ['NYC_FB200_ACCESS_KEY']
fb_secret_access_key = os.environ['NYC_FB200_SECRET_ACCESS_KEY']
fb_endpoint = 'http://10.234.112.148'
ingest_bucket = 'finnhub-ingest'
raw_bucket = 'finnhub-raw'
error_bucket = 'finnhub-errors'

# Main point of control for the Consumer module, handles all functions call to ingest data form DataLake and send to Cassandra
def main():
    # Create connection to FlashBlade
    s3 = get_s3_resource(fb_access_key, fb_secret_access_key, fb_endpoint)
    
    # Get all objects in the finnhub-raw bucket, iterate through objects and concatenate into a list
    logger.info('Getting finnhub-ingest contents')
    objects = get_bucket_contents(s3, ingest_bucket)
    logger.info('Concat objects to Data List')     
    object_list = concat_finnhub_json_objects_to_list(objects, s3)
    if not object_list:
        logger.warning('No files to process')  
        return None
    
    # Convert list to a dataframe and send to converter for transforamtion
    logger.info('Converting Data List to DataFrame')
    df = pd.DataFrame(object_list)
    df = converter.convert_df(df)

    # Connect to Cassandra market DB and upload df, close connections
    logger.info('Loading DataFrame to Cassandra DB')
    loader.load_df(df)

    # Move objects to raw folder
    for object in objects:
        logger.debug('Moving objects to finnhub-raw bucket')
        move_s3_file(ingest_bucket, raw_bucket, object.key, s3)

def move_s3_file(source_bucket_name, dest_bucket_name, object_key, session):
    src_bucket = session.Bucket(source_bucket_name)
    dest_bucket = session.Bucket(dest_bucket_name)

    #Create a source dictionary that specifies bucket name and key name of the object to be copied
    copy_source = {
        'Bucket': source_bucket_name,
        'Key': object_key
    }

    #Creating destination bucket 
    destbucket = session.Bucket(dest_bucket_name)

    #Copying the object to the target directory
    destbucket.copy(copy_source, object_key)

    #Delete original object
    session.Object(source_bucket_name, object_key).delete() 

def get_s3_resource(fb_access_key, fb_secret_access_key, fb_endpoint):
    s3 = boto3.resource(
        's3',
        endpoint_url=fb_endpoint,
        aws_access_key_id=fb_access_key, 
        aws_secret_access_key=fb_secret_access_key
    )
    return s3

def get_bucket_contents(s3, bucket):
    # Get all the objects in a given bucket
    return s3.Bucket(bucket).objects.all()

def get_object_json_contents(json_object):
    # Return a JSON content block for a given object
    file_content = json_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content

def concat_finnhub_json_objects_to_list(objects, s3):
    data = []
    
    for object in objects:
        # Extract JSON content from the FinnHub message
        json_content = get_object_json_contents(object)
        logger.info('Object being processed: %s', object)
        logger.debug('JSON Content of Object: %s', json_content)

        # Extract ingestion timestamp from object name for additiona to data list
        ingest_timestamp = int(object.key.split('_')[0])

        # Iterate through all objects, generate UUID and add to data list
        try:
            for record in json_content['data']:
                id = uuid.uuid4() # Generate a unique ID for each trade
                data.append(
                    {'c':record['c'], 
                    'p':record['p'], 
                    's':record['s'], 
                    't':record['t'], 
                    'v':record['v'],
                    'ingest_timestamp':ingest_timestamp,
                    'uuid':id}
                    )
        except Exception as e: 
            print(e)
        # TODO Debug why this is causing failuresi nthe pipline and research proper way to handle exceptions in processing records from Finnhub
        #finally:
        #    logger.critical('Unable to process a file, moving to finnhub-errors bucket')
        #    logger.critical('FILENAME: %s', object.key)
        #   move_s3_file(ingest_bucket, error_bucket, object.key, s3)
    
    return data

if __name__ == '__main__': 
    # Run the main() function and then sleep for 15 seconds
    while(True):
        logger.debug('Consumer awake - Running main()!!!')
        main()
        logger.info('Consumer sleeping for 15 seconds zzzzzzzzzzzzzz')
        time.sleep(15)
