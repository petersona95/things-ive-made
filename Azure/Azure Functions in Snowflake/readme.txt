Goal was to convert our ETL tasks (start audit, end audit, etc) from Python Azure Functions to snowflake Stored Procedures. From here, call these stored procedures and use the outputs throughout the pipeline.

Azure is capable of processing and using the JSON return from Start Audit throughout the pipeline.

The pipeline will automatically fail at the end in order to properly test the write_etl_message Procedure.



Project consists of:

- Table DDL to support the Audit Procedures
- Start Audit Task - create a new record in audit table and return JSON to be used throughout the pipeline
- Query Snowflake - sends a plain text query (stored in blob) to Snowflake to be processed. Allows you to run DML without issue in Azure
- End Audit Task - Updates your audit record upon the pipeline completing successfully
- Write ETL Message - Writes the Azure error to your error log table. Appends the task_key to allow you to determine which pipes are failing and why