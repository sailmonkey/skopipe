import boto3 # for connecting to FlashBlade using Boto3
import json
import pandas as pd
import time # For looping script every 15 secs
import FinnhubCassandraConversion as converter
import CassandraLoader as loader
import uuid # for generating unique ID for each trade


#FB access
fb_access_key='PSFBSAZQHGDJCEAFGHLPOLCNGBACAEDAODGJNMCF'
fb_secret_access_key='E4E8963E3402016/555c7B2D10A3ed4a863bGFNP'
fb_endpoint = 'http://10.234.112.148'
raw_bucket = 'finnhub-raw'

def main():
    # Create connection to FlashBlade
    s3 = get_s3_resource(fb_access_key, fb_secret_access_key, fb_endpoint)
    
    # Get all objects in the finnhub-raw bucket, iterate through objects and concatenate into a list
    objects = get_bucket_contents(s3, raw_bucket)
    object_list = concat_finnhub_json_objects_to_list(objects)

    # Convert list to a dataframe and send to converter for transforamtion
    df = pd.DataFrame(object_list)
    df = converter.convert_df(df)

    # TODO Connect to Cassandra market DB and upload df, close connections
    loader.load_df(df)

    # TODO Move objects to processed folder (or mark as .done)

def get_s3_resource(fb_access_key, fb_secret_access_key, fb_endpoint):
    s3 = boto3.resource(
        's3',
        endpoint_url=fb_endpoint,
        aws_access_key_id=fb_access_key, 
        aws_secret_access_key=fb_secret_access_key
    )
    return s3

def get_bucket_contents(s3, bucket):
    return s3.Bucket(bucket).objects.all()

def get_object_json_contents(json_object):
    file_content = json_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content

def concat_finnhub_json_objects_to_list(objects):
    data = []
    
    for object in objects:
        # Extract JSON content from the FinnHub message
        json_content = get_object_json_contents(object)

        # Extract ingestion timestamp from object name for additiona to data list
        ingest_timestamp = int(object.key.split('_')[0])

        # Iterate through all objects, generate UUID and add to data list
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
    
    return data

if __name__ == '__main__': 
    main()
    #while(True):
    #    main()
    #    time.sleep(15)
