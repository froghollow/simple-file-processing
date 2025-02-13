"""
## glue_functions.py -- Common code module for DUDE Glue Utilities
#  (by RAMyers, DAAB.BSSD@fiscal.treasury.gov, 04/20/2022)
"""

import os
import sys
import json
import datetime

if 'AWS_DEFAULT_REGION' not in os.environ.keys():
    os.environ['AWS_DEFAULT_REGION'] = 'us-gov-west-1'

# JSON template for Glue table structure (e.g., s3://{env-prefix}-code/config/glue_table_input_template.json)
if 'GlueTableInputTemplateUrl' not in os.environ.keys():
    os.environ['GlueTableInputTemplateUrl'] = 'file://data/glue_table_input_template.json'

import boto3
glue = boto3.client('glue')
s3 = boto3.client('s3')

def list_glue_tables( glue_database_name, regex = '.'):
    ''' List Glue Database Tables filtered by RegEx '''
    import os
    import boto3
    import re

    #os.environ['AWS_DEFAULT_REGION'] = 'us-gov-west-1'
    regex = re.compile( regex )

    # get list of Glue tables
    glue = boto3.client('glue')
    response = glue.get_tables(
        #CatalogId='string',
        DatabaseName=glue_database_name,
        #Expression='string',
        #NextToken='string',
        #MaxResults=123
    )
    table_list = response['TableList']
    while 'NextToken' in response:
        response = glue.get_tables(
            DatabaseName=glue_database_name,
            NextToken = response['NextToken']
        )
        table_list.extend(response['TableList'])

    tables = []
    table_names = []
    for table in table_list:
        #print(table)
        if regex.search( table['Name'] ):
            #print('{}\t{}\t{}'.format(table['Name'], table['StorageDescriptor']['Location'], ''))
            #print('{}\t{}\t{}'.format(table['Name'], '', ''))
            tables.append( table )
            table_names.append( table['Name'] )

    return tables, table_names

def crup_glue_table( glue_database_name, glue_table_name, glue_sd_columns ):
    ''' CReate or UPdate Glue Table '''

    glue_db = glue.get_database( Name = glue_database_name )

    # convert to Glue-compatible data types
    tds_to_glue = {
        'big_int'   : 'bigint',
        'bytes'     : 'byte',
        'char'      : 'string',
        'date'      : 'timestamp',
        'smallint'  : 'short',
        'text'      : 'string',
    }
    for sd_col in glue_sd_columns:
        if  sd_col['Type'] in tds_to_glue.keys():
            sd_col['Type'] =  tds_to_glue[sd_col['Type']]

    glue_table_input['TableInput']['Name'] = glue_table_name
    glue_table_input['TableInput']['StorageDescriptor']['Columns'] = glue_sd_columns
    if 'LocationUri' in glue_db['Database'].keys():
        glue_table_input['TableInput']['StorageDescriptor']['Location'] = glue_db['Database']['LocationUri'] + '{}/'.format(glue_table_name.upper())

    try:
        response = glue.create_table (
            DatabaseName = glue_database_name,
            TableInput = glue_table_input['TableInput']
        )
        status = 'created'
    except glue.exceptions.AlreadyExistsException:
        response = glue.update_table (
            DatabaseName = glue_database_name,
            TableInput = glue_table_input['TableInput']
        )
        status = 'updated'

    print(f"Glue Table {glue_table_name} {status} in Database {glue_database_name}" )

    return response

def crup_glue_partition( glue_database_name, glue_table_name, partition_keys ):
    glue_partition_sd = glue_table['StorageDescriptor'].copy()
    glue_partition_sd['Location'] += partition_key + '/'

    try:
        response = glue.create_partition(
            DatabaseName = glue_database_name,
            TableName = glue_table['Name'] ,
            PartitionInput = {
                'Values' : [ partition_key ],
                'StorageDescriptor' : glue_partition_sd,
                'Parameters' : glue_table['Parameters']
            }
        )
        status = 'Created'

    except glue.exceptions.AlreadyExistsException:
        #print('updating ...')
        response = glue.update_partition(
            DatabaseName = glue_database_name,
            TableName = glue_table['Name'] ,
            PartitionValueList = partition_keys ,
            PartitionInput = {
                'Values' : [ partition_key ],
                'StorageDescriptor' : glue_partition_sd,
                'Parameters' : glue_table['Parameters']
            }
        )
        status = 'Updated'

    except Exception as e:
        status = e

    print(f"Partitions {status} for Glue Table '{glue_table_name}' in Database '{glue_database_name}: {partition_keys}'" )



def crup_glue_date_partitions( glue_database_name, glue_tables_list, partkey_format='D%y%m%d.Full', beg_date='', end_date='' ):
    ''' CReate or UPdate Glue Table(s) Set of Date Partitions '''

    delta = datetime.timedelta(days=1)

    if beg_date == '':
        beg_date = datetime.datetime.today()
        beg_date = datetime.date(beg_date.year, beg_date.month, beg_date.day)

    if end_date == '':
        end_date = beg_date
    #print (beg_date , end_date, beg_date <= end_date)

    print( glue_database_name + ' add date partitions ...')

    if glue_tables_list == []:    # default all tables in database
        glue_tables_list, table_names = list_glue_tables( glue_database_name )

    while beg_date <= end_date:
        for glue_table in glue_tables_list:
            partition_key = beg_date.strftime(partkey_format)
        beg_date += delta

def list_glue_partitions( glue_database_name, glue_table_name, regex = '.', mode='List' ):
    ''' List (Delete) a set of Glue Partions filtered by RegEx  '''
    regex = re.compile( regex )

    response = glue.get_partitions(
        DatabaseName = glue_database_name,
        TableName = glue_table_name
    )
    partition_list = response['Partitions']
    #print (response)
    #break
    while 'NextToken' in response:
        print(response['NextToken'])
        response = glue.get_partitions(
            DatabaseName = glue_database_name,
            TableName = glue_table_name ,
            NextToken = response['NextToken']
        )
        partition_list.extend(response['Partitions'])

    partitions = []
    partition_names = []
    print(f"{mode} Partitions for Glue Database {glue_database_name}")
    for partition in partition_list:
        partition_name = partition['Values'][0]
        if regex.search( partition_name ):
            print( partition_name, partition['StorageDescriptor']['Location'] )
            partitions.append( partition )
            partition_names.append( partition_name )
            if mode == 'Delete':
                #continue
                #print('Deleting ...')
                response = glue.delete_partition(
                    DatabaseName = glue_database_name,
                    TableName = glue_table_name,
                    PartitionValues = partition['Values']
                )
                #print(response)

    return partitions, partition_names

def export_glue_to_raml( glue_database_name, glue_table_name, output_path='' ):
    ''' Export Glue table metadata to Mule-compatible RAMLv1.0 '''
    import yaml

    tables, table_names = list_glue_tables( glue_database_name, glue_table_name )

    table = tables[0]

    dt_properties = {}
    for sd_column in table['StorageDescriptor']['Columns']:

        #print( sd_column['Name'] , sd_column['Type'])

        # convert from Glue to RAML scalar type
        column_type = sd_column[ 'Type' ]
        if column_type == 'timestamp':
            column_type = 'datetime'
        elif column_type in ('byte','short','long'):
            column_type = 'integer'
        elif column_type in ('float','double','decimal'):
            column_type = 'number'

        sd_column_comment = ''
        if 'Comment' in sd_column.keys():
            sd_column_comment = sd_column[ 'Comment' ]

        dt_properties [ sd_column['Name'] ] = {
            "type" : column_type,
            "displayName" : sd_column['Name'].replace('_', " ").title(),
            "description" : sd_column_comment
        }

        if 'Parameters' in sd_column.keys():
            '''
            for key,val in sd_column['Parameters']:     # map Glue custom parms to RAML type properties
                dt_properties [sd_column['Name'][key]] = val
            '''
            for parm in sd_column['Parameters']:     # map Glue custom parms to RAML type properties
                #print(parm, sd_column['Parameters'][parm])
                dt_properties [sd_column['Name']] [ parm ] = sd_column['Parameters'][parm]

    if 'Description' in table.keys():
        table_desc = table['Description']
    else:
        table_desc = f'Exported from Glue Catalog Table {glue_table_name}'
        
    datatype_json = { glue_table_name : {
        "type" : "object",
        "displayName" : glue_table_name.replace('_', " ").title(),
        "description" : table_desc,
        "properties" : dt_properties
        }
    }

    yaml_template = "#%RAML 1.0\n\ntitle: RAML Export from Glue Catalog\n"

    datatype_json = {"types" : datatype_json}

    raml = yaml_template + yaml.dump(datatype_json, sort_keys=False)
    if output_path > '':
        put_file( output_path , raml, 'w')
        print( f"Output to file '{output_path}'" )
        
    return raml

def import_raml_to_glue( glue_database_name, glue_table_name, raml, output_path='' ):
    ''' Import RAMLv1.0 Data Type into Glue Catalog '''
    from copy import deepcopy
    import yaml
    json_spec = yaml.safe_load(raml)
    
    glue_sd_columns = []
    for type in json_spec['types']:
        params = {}
        glue_sd_column = {
            "Name" : "",
            "Type" : "",
            "Comment" : "",
            "Parameters" : {}
        }
        for col_key, col_values in json_spec['types'][type]['properties'].items():
            glue_sd_column['Name'] = col_key
            params = {}
            for prop_key, prop_value in col_values.items():
                if prop_key == 'type':
                    if prop_value == 'number':  # convert to Glue type 
                        prop_value = 'double'
                    elif prop_value == 'integer':
                        prop_value = 'int'
                    glue_sd_column['Type'] = prop_value
                elif prop_key == 'description':
                    glue_sd_column['Comment'] = prop_value[:254] 
                    if len(prop_value) > 254:
                        params['LongDescription'] = prop_value
                else:
                    if prop_value == True: prop_value = 'true'
                    if prop_value == False: prop_value = 'false'
                    
                    params[prop_key] = prop_value
            glue_sd_column['Parameters'] = params.copy()
           
            glue_sd_columns.append(glue_sd_column.copy() )

        #print(glue_sd_columns)
    if output_path > '':
        put_file( output_path , json.dumps(glue_sd_columns, indent=2 ), 'w')
        print( f"Output to file '{output_path}'" )

    return json.loads(json.dumps(glue_sd_columns))

####### FILE UTILITY FUNCTIONS (copy from batch_functions) #########
import re
import shutil

def copy_file(source_file, dest_file):
    """ Copy source File or S3 object to destination File or S3 object """
    source_vars = parse_filename(source_file)
    dest_vars = parse_filename(dest_file)

    try:
        if source_vars['FileUriLoc'] == dest_vars['FileUriLoc'] == 's3://':
            copy_source = { 'Bucket': source_vars['Bucket'], 'Key': source_vars['Key'] }
            s3.copy_object(CopySource=copy_source, Bucket=dest_vars['Bucket'], Key=dest_vars['Key'])
            rc = '200'
        elif source_vars['FileUriLoc'] == dest_vars['FileUriLoc'] == 'file://':
            shutil.copy( source_vars['FileName'], dest_vars['FileName'] )
            rc = '200'
        else:
            data = get_file(source_file)
            rc = put_file(dest_file, data)
            #rc = '200'

    except Exception as e:
        print(['ERROR: Unable to copy file {} to {}'.format(source_file, dest_file), e])
        rc = '500'

    return rc

def delete_file(file_name):
    """ Delete File or S3 object """
    vars = parse_filename(file_name)
    try:
        if vars['FileUriLoc'] == 's3://':
            resp = s3.delete_object(Bucket=vars['Bucket'], Key=vars['Key'])
            rc = '200'
        elif vars['FileUriLoc'] == 'file://':
            if os.path.exists(vars['FileName']):
                os.remove(vars['FileName'])
                rc = '200'
            else:
                print("The file does not exist: {}".format(vars['FileName']))
                rc = '400'
        else:
            print("Unknown File Type for {}.  Must be prefixed with 'file://' or 's3://'".format(file_name))
            rc = '400'

    except Exception as e:
        print(['ERROR: Unable to delete {}'.format(file_name), e])
        rc = '500'

    return rc

def get_file(file_name):
    """ Get File or S3 object contents """
    vars = parse_filename(file_name)
    try:
        if vars['FileUriLoc'] == 's3://':
            s3_obj = s3.get_object(Bucket=vars['Bucket'], Key=vars['Key'] )
            data = s3_obj['Body'].read().decode('utf-8')
        elif vars['FileUriLoc'] == 'file://':
            with open( vars['FileName'] ) as f:
                data = f.read()
            f.close()
        else:
            print("Unknown File Type for {}.  Must be prefixed with 'file://' or 's3://'".format(file_name))
            data = '400'

    except Exception as e:
        print(['ERROR: Unable to get {}'.format(file_name), e])
        data = '500'

    return data

def move_file(source_file, dest_file):
    """ Copy source File or S3 object to destination File or S3 object, then delete source """
    source_vars = parse_filename(source_file)
    dest_vars = parse_filename(dest_file)

    try:
        if source_vars['FileUriLoc'] == dest_vars['FileUriLoc'] == 'file://':
            shutil.move( source_vars['FileName'], dest_vars['FileName'] )
            rc = '200'
        else:
            rc = copy_file(source_file, dest_file)
            if rc == '200':
                rc = delete_file(source_file)

    except Exception as e:
        print(['ERROR: Unable to move file {} to {}'.format(source_file, dest_file), e])
        rc = '500'

    return rc

def put_file(file_name, data, mode='a'):
    """ Overwrite file or S3 content with data """
    vars = parse_filename(file_name)
    try:
        if vars['FileUriLoc'] == 's3://':
            resp = s3.put_object(Bucket=vars['Bucket'], Key=vars['Key'], Body=data)
            rc = '200'
        elif vars['FileUriLoc'] == 'file://':
            if not os.path.isdir( vars['Directory'] ):
                os.makedirs( vars['Directory'] )
            with open( vars['FileName'], mode ) as f:
                resp = f.write(data)
            f.close()
            rc = '200'
        else:
            print("Unknown File Type for {}.  Must be prefixed with 'file://' or 's3://'".format(file_name))
            rc = '400'

    except Exception as e:
        print(['ERROR: Unable to put {}'.format(file_name), e])
        rc = '500'

    return rc

def parse_filename(filename):
    """ Parse URI into 's3://' Bucket & Key or 'file://' Directory & FileName """
    vars = {
        "FileInput" : filename
    }
    if '//' in filename:
        fname = filename[filename.find('//')+2:]
        if filename.startswith('s3://'):
            vars.update( {
                "Bucket": fname[:fname.find('/')],
                "Key": fname[fname.find('/')+1:],
                "FileUriLoc": "s3://"
            } )
        elif filename.startswith('file://'):
            vars.update( {
                "FileName" : fname,
                "Directory" : fname[:fname.rfind('/')],
                "FileUriLoc": "file://"
            } )
    return vars

glue_table_input = json.loads( get_file( os.environ['GlueTableInputTemplateUrl'] ))

'''
### SAMPLE DRIVER
os.environ['GlueTableInputTemplateUrl'] = f's3://{env-prefix}-code/config/glue_table_input_template.json'

glue_database_name = "dtl-prd-tdsx"
glue_table_name = "detailed_top_data_source"
output_path = f's3://{env-prefix}-datawork/rmyers07/TDSX/PROCESSED/DETAILED_TOP_DATA_SOURCE/{}.raml'.format(glue_table_name)
yaml = export_glue_to_raml( glue_database_name, glue_table_name, output_path )

### SAMPLE DRIVER
'''
