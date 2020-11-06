'''
This lambda function is capable of scraping an s3 bucket with directory /dev/[table_name]/[file_name], 
collecting all of those json objects, and dynamically checking to see the columns needed for table ddl.

This script was used to collect all of the "columns" output from the Mavenlink API. sometimes, the columns for two invoices,
two users, two etc are not consistent with each other.

1. connects to s3
2. gets a list of all the "folders" aka the root directories for each file type
3. loops over these folders and collects the table DDL for each item type (ex: all invoices). 
    a. appends column names to a list, only passing in unique columns
    b. alphabetically sorts columns names
    c. adds column names and table_name to a combined dictionary with all tables 
4. parses dictionary to create CREATE OR REPLACE TABLE() statements
    a. creates DDL for target tables
    b. creates DDL for stage tables
5. Outputs all table DDL into a .txt file in the root folder of the bucket.
    a. user must manually run this statement on Snowflake instance
    
PROJECT TECH STACK:
Mavenlink Developer API
Lambda
s3
Snowflake
'''

import json
import boto3

def lambda_handler(event, context):
    conn = boto3.client('s3')
    bucket = <ENTER BUCKET NAME> ##EX: 'bucket1'
    prefix = <ENTER PREFIX> ## 'foo/bar'
      
    #collect list of folder names in s3 bucket
    folder_list = get_folder_list(conn, bucket, prefix)
    
    #get dictionary of all columns for each table
    table_ddl = get_column_ddl_dict(conn, bucket, prefix, folder_list)
    
    #generate a string with all create or replace table_ddl
    sql_stmt = create_sql_stmt(table_ddl)
    
    #write this file to s3
    conn.put_object(Bucket=bucket, Key='mavenlink-api/table_ddl.txt', Body=sql_stmt)
    
def get_folder_list(conn, bucket, prefix):
    
    json_folder_list = []
    result = conn.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
    for f in result.get('CommonPrefixes'):
        dir = f.get('Prefix').replace(prefix, '')
        json_folder_list.append(dir)
    return json_folder_list


def get_column_ddl_dict(conn, bucket, prefix, folder_list):
    #folder_list = get_folder_list(conn, bucket, prefix)
    
    #initialize dictionary to store columns and table names
    table_ddl = {}
    for folder in folder_list:
        #folder looks like this: account_colors/
        prefix_folder = prefix + folder
        table_name = folder.replace('/','')
        #list to collect column names
        columns_list = []
        for key in conn.list_objects(Bucket=bucket, Prefix=prefix_folder)['Contents']:
            ##get the content
            response = conn.get_object(Bucket = bucket, Key = key['Key'])
            content = response['Body']
            ##load the entire json object
            jsonObject = json.loads(content.read())
            ##get only the table_name key, account_colors{}
            table_type = jsonObject[table_name.lower()]
            ##get a list of all the keys (id 91111, id 92222)
            key_list = list(table_type.keys())
            for id in key_list:
                columns_per_id = table_type[id]
                #col is actual column name
                for col in columns_per_id:
                    #pass in only unique columns
                    #we will add 'id' column to the end
                    if col not in columns_list and col != 'id':
                        columns_list.append(col)
                        
        #sort the column list alphabetically
        columns_list = sorted(columns_list, key=str.lower)
        #add id and source_file to front of the list
        columns_list.insert(0,'source_file')
        columns_list.insert(0,'ID')
        table_ddl[table_name] = columns_list
    #get only unique values from that list
    return table_ddl

def create_sql_stmt(table_ddl):
    #tables are table names, aka keys of dict
    #create target tables
    target_stmt = ''
    for table_name in [*table_ddl]:
        col_list = table_ddl[table_name]
        col_stmt = ' varchar(5000), \n\t'.join(col_list)
        start_stmt = 'create or replace table DISCOVERY_RAW.MLD.{table} ( \n\t'.format(table=table_name)
        sql_stmt = start_stmt + col_stmt + ' varchar(5000)' + '\n);\n'
        target_stmt = target_stmt + sql_stmt
        
    stg_stmt = ''
    for table_name in [*table_ddl]:
        col_list = table_ddl[table_name]
        col_stmt = ' varchar(5000), \n\t'.join(col_list)
        start_stmt = 'create or replace table DISCOVERY_RAW.STAGE.MLD_{table} ( \n\t'.format(table=table_name)
        sql_stmt = start_stmt + col_stmt + ' varchar(5000)' + '\n);\n'
        stg_stmt = stg_stmt + sql_stmt
    
    #combine target and stage ddl
    combined_stmt = target_stmt + '\n' + stg_stmt
    
    #replace current with "current" , current is not an allowed column in SF
    current = '"' + 'current' + '"' + ' '
    combined_stmt = combined_stmt.replace('current ', current)
    return combined_stmt