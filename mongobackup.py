import datetime as date
import urllib.parse as urlparse
import argparse 
import subprocess
import boto3
from sys import exit
import logging
import tarfile 
import os
from shutil import rmtree


logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description='Backup Mongolab DBs')
parser.add_argument('-b','--bucket', help='URL of the bucket', required=True, type=str)

parser.add_argument('-d','--destination', help='Destination of mongobackup', required=True, type=str)

def mongodump(destination):
  
    today = date.datetime.now().strftime('%Y-%m-%d')
    
    if os.path.isdir(destination):
        backuploc = os.path.join(destination,'mongobackup') + '-' + today
        command = ['mongodump','-o',backuploc]
    else:  
        logging.info('Destination not present')
        exit()
    
    try: 
        process = subprocess.check_output(command, stderr=subprocess.STDOUT)
        
    except subprocess.CalledProcessError as processerror:
        logging.info(processerror.output.decode('utf-8'))
        exit()
    with tarfile.open(backuploc+'.tgz',mode='w:gz') as tar:
        tar.add(backuploc)
    
    try:
        rmtree(backuploc)
    except FileNotFoundError as rmerror:
        logging.info(rmerror)

    return backuploc+'.tgz'


def uploadmongodump(bucket,src):

    bucketNAME = urlparse.urlparse(bucket)
    s3 = boto3.resource('s3')

    print("The bucket name is {}".format(bucketNAME.netloc))

    if not s3.Bucket(bucketNAME.netloc) in s3.buckets.all():
        logging.info('Bucket not found')
        exit()

    key=os.path.basename(src)
    print(key)
    print(bucketNAME.netloc)
    s3.Bucket(bucketNAME.netloc).upload_file(src,key)
    
    try:
        os.remove(src)
    except FileNotFoundError as rmerror:
        logging.info(rmerror)

    
if __name__ == '__main__':
    
    arg = parser.parse_args()
    dest = arg.destination
    target = mongodump(dest)

    bucket = arg.bucket
    
    uploadmongodump(bucket,target)
    
