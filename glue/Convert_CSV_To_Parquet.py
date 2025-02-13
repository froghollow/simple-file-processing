import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME","ProcessParms"])
#print(args)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

import json
batch_parms_s = args['ProcessParms']
#print(batch_parms_s)
batch_parms = json.loads(batch_parms_s)

glue_database_name = batch_parms['GlueDatabaseName']
#s3_bucket = batch_parms['S3OutputBucket']
#s3_input_folder  = batch_parms['S3InputFolder']
#s3_output_folder = batch_parms['S3OutputFolder']
s3_input_bucket  = batch_parms['S3LandingPadBucket']
s3_input_folder  = batch_parms['S3LandingPadInput']
s3_output_bucket = batch_parms['S3DatalakeBucket']
s3_output_folder = batch_parms['S3DatalakeOutput']
glue_table_names = batch_parms['ZipExtracted']['GlueTableNames']
partition_folders= batch_parms['ZipExtracted']['PartitionFolders']

# ToDo: source from common glue_functions.py ...
# from glue_functions import crup_glue_partition
def crup_glue_partition( glue_database_name, glue_table_name, partition_keys ):
    ''' CReate or UPdate Glue Partition '''
    glue_table = glue_client.get_table(
        DatabaseName = glue_database_name,
        Name = glue_table_name
    )
    glue_partition_sd = glue_table['Table']['StorageDescriptor'].copy()
    glue_partition_sd['Location'] = f"{glue_partition_sd['Location']}/{partition_keys[0]}"
    
    partition_input = {
        'Values' : partition_keys,
        'StorageDescriptor' : glue_partition_sd,
        'Parameters' : glue_table['Table']['Parameters']
    }
    try:
        response = glue_client.create_partition(
            DatabaseName = glue_database_name,
            TableName = glue_table_name ,
            PartitionInput = partition_input
        )
        status = 'Created'

    except glue_client.exceptions.AlreadyExistsException:
        response = glue_client.update_partition(
            DatabaseName = glue_database_name,
            TableName = glue_table_name ,
            PartitionValueList = partition_keys,
            PartitionInput = partition_input
        )
        status = 'Updated'

    print(f"Partitions {status} for Glue Table '{glue_table_name}' in Database '{glue_database_name}: {partition_keys}'" )

    return glue_partition_sd

import boto3
glue_client = boto3.client('glue')

for glue_table_name in glue_table_names:

    glue_table = glue_client.get_table(
        DatabaseName = glue_database_name,
        Name = glue_table_name
    )
    columns = glue_table['Table']['StorageDescriptor']['Columns']
    str_to_sd_mappings = []
    for column in columns:
        # CSV files wherein all values are enclosed in "" ...
        str_to_sd_mappings.append ( (column['Name'], 'string', column['Name'], column['Type']) )

    for partition_folder in partition_folders:
        # Script generated for node S3 bucket
        s3_input_df = glueContext.create_dynamic_frame.from_options(
            format_options={
                "quoteChar": '"',
                "withHeader": True,
                "separator": ",",
                "optimizePerformance": False,
            },
            connection_type="s3",
            format="csv",
            connection_options={
                "paths": [
                    f"s3://{s3_input_bucket}/{s3_input_folder}/{glue_table_name}/{partition_folder}/"
                ],
                "recurse": True,
            },
            transformation_ctx="s3_input_df",
        )
        # Script generated for node ApplyMapping
        apply_str_to_sd_mappings = ApplyMapping.apply(
            frame=s3_input_df,
            mappings = str_to_sd_mappings,
            transformation_ctx="ApplyMapping_node2",
        )
        # Script generated for node S3 bucket
        s3_output_sink = glueContext.write_dynamic_frame.from_options(
            frame=apply_str_to_sd_mappings,
            connection_type="s3",
            format="glueparquet",
            connection_options={
                "path": f"s3://{s3_output_bucket}/{s3_output_folder}/{glue_table_name}/{partition_folder}/",
                "partitionKeys": [],
            },
            format_options={"compression": "snappy"},
            transformation_ctx="s3_output_sink",
        )
        crup_glue_partition( glue_database_name, glue_table_name, [ partition_folder ] )

job.commit()