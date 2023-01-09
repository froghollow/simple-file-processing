import json
import os
import zipfile
import gzip
import boto3
import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    ''' Download ZIP archive file from S3, extract it, and upload GZip'd member files to S3 '''
    print("Received event: " + json.dumps(event))
    response = event
    
    if 'source' in event.keys():
        # triggered by EventBridge ...
        if event['source'] == 'aws.s3':
            s3_bucket = event['detail']['bucket']['name']
            s3_key = event['detail']['object']['key']
    #elif 'Records' in event.keys():
    #    # triggered by S3 ObjectCreated event ... (deprecated 2212)
    #    if event['Records'][0]['eventSource'] == 'aws:s3':  
    #        s3_bucket = event['Records'][0]['s3']['bucket']['name']
    #        s3_key = event['Records'][0]['s3']['object']['key']
    else:
        # ... or simple event from elsewhere
        s3_bucket = event['S3BucketName']
        s3_key = event['S3Key']
    
    s3_source_url = f's3://{s3_bucket}/{s3_key}'

    work_zipfile_name = s3_source_url.split('/')[-1]
    s3_source_folder = s3_key[:s3_key.rfind('/')]
    if 'S3ExtractFolder' in event['process_parms'].keys():
        s3_target_folder = event['process_parms']['S3ExtractFolder'] 
    else:
        s3_target_folder = s3_source_folder

    now = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    if 'WorkFolder' not in os.environ.keys():
        os.environ['WorkFolder'] = '/tmp/unzip'     # Lambda default only 512M -- mount EFS to support large archive files
    work_folder = f"{os.environ['WorkFolder']}/{now}"
    if not os.path.exists(work_folder):
        os.makedirs( work_folder )
    
    s3_client.download_file(s3_bucket, s3_key, f"{work_folder}/{work_zipfile_name}")
    print(f"Downloaded '{s3_source_url}' to '{work_folder}/{work_zipfile_name}'")
    
    with zipfile.ZipFile(f"{work_folder}/{work_zipfile_name}", mode="r") as archive:
        archive.printdir()
        infolist = archive.infolist()
        zip_extracted = {
            "GlueTableNames" : [],
            "PartitionFolders" : [],
            "S3ExtractedUrls" : []
        }
        
        for member in infolist:
            member_filename = member.filename 
            # ref https://www.tutorialspoint.com/python-support-for-gzip-files-gzip
            extract_path = archive.extract(member , work_folder )
            with open ( extract_path, "rb" ) as ext:
                bindata = ext.read()
                with gzip.open ( f"{extract_path}.gz", "wb") as gz:
                    gz.write( bindata )
                    
            table_folder = member_filename.replace('.','/',1).replace('.csv','')
            s3_client.upload_file( f"{extract_path}.gz", s3_bucket, f'{s3_target_folder}/{table_folder}/{member_filename}.gz' )
            s3_target_url = f"s3://{s3_bucket}/{s3_target_folder}/{table_folder}/{member_filename}.gz"
            print(f"Uploaded '{extract_path}' to '{s3_target_url}'")

            zip_extracted['GlueTableNames'].append( table_folder.split('/')[0] )
            zip_extracted['S3ExtractedUrls'].append( s3_target_url )
            partition_folder = table_folder.split('/')[1]
            if partition_folder not in zip_extracted['PartitionFolders']:
                zip_extracted['PartitionFolders'].append( partition_folder )

            os.remove( extract_path )
            os.remove( f"{extract_path}.gz" )

    response['process_parms']['S3InputFolder'] = s3_source_folder
    response['process_parms']['ZipExtracted']  = zip_extracted

    import shutil
    shutil.rmtree( work_folder ) #use shutil b/c os.removedirs( work_folder ) only works when empty
    
    print("Response: " + json.dumps(response))
    return {
        'statusCode': 200,
        'body':  response
    }
