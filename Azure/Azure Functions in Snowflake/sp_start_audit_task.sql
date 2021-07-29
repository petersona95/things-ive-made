CREATE OR REPLACE PROCEDURE APT_DW.AUDIT.SP_START_AUDIT_TASK(AUDIT_DB VARCHAR, DEST_DB VARCHAR, DEST_SCHEMA VARCHAR
, DEST_TBL VARCHAR, DEST_VIEW VARCHAR, SOURCE_DB VARCHAR, SOURCE_SCHEMA VARCHAR, SOURCE_TBL VARCHAR, PRIMARY_JOIN_KEYS VARCHAR
, PIPELINE_RUN_ID VARCHAR, PARENT_TASK_KEY VARCHAR, TASK_NAME VARCHAR, TASK_GROUP VARCHAR, CDC_TYPE VARCHAR, FULL_LOAD_IND VARCHAR
, SQL_CONTAINER VARCHAR, SQL_DIRECTORY VARCHAR, SQL_INSERT_FILE_NAME VARCHAR, SOURCE_TYPE VARCHAR, BLOB_CONTAINER VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$

    //TRY CATCH IS COMMENTED OUT EVERYWHERE BECAUSE WE CANNOT PROPOGATE A FAILURE TO AZURE

    //INITIALIZE THE RETURN IN CASE OF FAILURES
    result = {
            "TaskName" : TASK_NAME,
            "TaskGroup" : TASK_GROUP,
            "TaskKey" : "-1",
            "CDCType" : CDC_TYPE,
            "CDC_MIN_DATE" : "1/1/1900",
	        "CDC_MAX_DATE" : "12/31/9999", 
            "SYS_CHANGE_VERSION_START" : "-1",
            "SYS_CHANGE_VERSION_END" : "-1",    
            "SQLContainer" : SQL_CONTAINER,
            "SQLDirectory" : SQL_DIRECTORY,
            "SQLInsertFileName" : SQL_INSERT_FILE_NAME,
            "PRIMARY_SOURCE_DATABASE_NAME" :  SOURCE_DB,
            "PRIMARY_SOURCE_SCHEMA_NAME" :  SOURCE_SCHEMA,
            "PRIMARY_SOURCE_TABLE_NAME" :  SOURCE_TBL,
            "PRIMARY_SOURCE_JOIN_KEYS" :  PRIMARY_JOIN_KEYS,
            "DESTINATION_DATABASE_NAME" :  DEST_DB,
            "DESTINATION_SCHEMA_NAME" :  DEST_SCHEMA,
            "DESTINATION_TABLE_NAME" :  DEST_TBL,
            "DESTINATION_VIEW_NAME" : DEST_VIEW,
            "SourceType" : SOURCE_TYPE,
            "BlobContainer" : BLOB_CONTAINER,
	    "AuditDB": AUDIT_DB,
            "Status" : "Unknown",
            "ErrorMessage" : "None"
            }

    //GET INITIAL ROW COUNT FROM THE TABLE
    sql_stmt = `SELECT COUNT(1) AS TARGET_INITIAL_ROW_COUNT FROM  ` + DEST_DB + `.` + DEST_SCHEMA + `.` + DEST_TBL;
    sql_stmt = snowflake.createStatement({sqlText: sql_stmt});
    //try {
        row_count = sql_stmt.execute();
        row_count.next();
        row_count = row_count.getColumnValue(1);
    //} catch(err) {
    //    err_msg =  `Failed to get Row Count: Code: ` + err.code + `\n  State: ` + err.state;
    //    err_msg += `\n  Message: ` + err.message;
    //    err_msg += `\nStack Trace:\n` + err.stackTraceTxt;
    //
    //    //APPEND ERROR TO JSON RESPONSE
    //    result["Status"] = "Failure";
    //    result["ErrorMessage"] = err_msg;
    //    return result;
    //}

    // INSERT NEW RECORD INTO AUDIT TABLE
    sql_stmt = `
                INSERT INTO ` + AUDIT_DB + `.AUDIT.ETL_TASK (
                TASK_KEY
                , PARENT_TASK_KEY
                , TASK_NAME
                , TASK_GROUP
                , EXECUTION_START_DATE
                , CDC_TYPE
                , CDC_MIN_DATE
                , CDC_MAX_DATE
				, SYS_CHANGE_VERSION_START
                , PRIMARY_SOURCE_DATABASE_NAME
                , PRIMARY_SOURCE_SCHEMA_NAME
                , PRIMARY_SOURCE_TABLE_NAME
                , TARGET_INITIAL_ROW_COUNT
                , SOURCE_TYPE
                , DESTINATION_DATABASE_NAME
                , DESTINATION_SCHEMA_NAME
                , DESTINATION_TABLE_NAME
                , DESTINATION_OBJECT_VIEW_NAME
                , PIPELINE_RUN_ID
            )
            WITH SYS_CHANGE_VERSION_START AS (
                SELECT NEW_CDC_MIN_DATE, NEW_SYS_CHANGE_VERSION_START
                FROM (SELECT 1 as i) init LEFT JOIN (
                    SELECT tsk.SYS_CHANGE_VERSION_END AS NEW_SYS_CHANGE_VERSION_START
                        ,tsk.CDC_MAX_DATE AS NEW_CDC_MIN_DATE
                    FROM  ` + AUDIT_DB + `.AUDIT.ETL_TASK tsk
                    INNER JOIN (
                    SELECT PRIMARY_SOURCE_DATABASE_NAME, PRIMARY_SOURCE_SCHEMA_NAME,
                        PRIMARY_SOURCE_TABLE_NAME, MAX(EXECUTION_STOP_DATE) AS EXECUTION_STOP_DATE
                    FROM  ` + AUDIT_DB + `.AUDIT.ETL_TASK
                    WHERE PRIMARY_SOURCE_DATABASE_NAME = '` + SOURCE_DB + `'
                    AND PRIMARY_SOURCE_SCHEMA_NAME = '` + SOURCE_SCHEMA + `'
                    AND PRIMARY_SOURCE_TABLE_NAME = '` + SOURCE_TBL + `'
                    AND SUCCESSFUL_PROCESSING_IND = True
                    GROUP BY PRIMARY_SOURCE_DATABASE_NAME, PRIMARY_SOURCE_SCHEMA_NAME, PRIMARY_SOURCE_TABLE_NAME
                    ) dt
                    ON tsk.PRIMARY_SOURCE_DATABASE_NAME = dt.PRIMARY_SOURCE_DATABASE_NAME
                    AND tsk.PRIMARY_SOURCE_SCHEMA_NAME = dt.PRIMARY_SOURCE_SCHEMA_NAME
                    AND tsk.PRIMARY_SOURCE_TABLE_NAME = dt.PRIMARY_SOURCE_TABLE_NAME
                    AND dt.EXECUTION_STOP_DATE = tsk.EXECUTION_STOP_DATE) audit
                ON 1=1)
            SELECT
             ` + AUDIT_DB + `.AUDIT.SEQ_ETL_TASK.nextval
            , '` + PARENT_TASK_KEY + `'
            , '` + TASK_NAME + `'
            , '` + TASK_GROUP + `'
            , current_timestamp::timestamp_ntz(9) 
            , '` + CDC_TYPE + `'
	        , COALESCE(NEW_CDC_MIN_DATE, '1900-01-01') 
            , current_timestamp::timestamp_ntz(9) 
	        , COALESCE(NEW_SYS_CHANGE_VERSION_START, -1) 
            , '` + SOURCE_DB + `'
            , '` + SOURCE_SCHEMA + `'
            , '` + SOURCE_TBL + `'
            , ` + row_count + `
            , '` + SOURCE_TYPE + `'
            , '` + DEST_DB + `'
            , '` + DEST_SCHEMA + `'
            , '` + DEST_TBL + `'
            , '` + DEST_VIEW + `'
            , '` + PIPELINE_RUN_ID + `'
            FROM SYS_CHANGE_VERSION_START;`;
    sql_stmt = snowflake.createStatement({sqlText: sql_stmt});
    //try {
        sql_stmt.execute();
    //} catch(err) {
    //    err_msg =  `Failed to insert new record: Code: ` + err.code + `\n  State: ` + err.state;
    //    err_msg += `\n  Message: ` + err.message;
    //    err_msg += `\nStack Trace:\n` + err.stackTraceTxt;
    //
    //    //APPEND ERROR TO JSON RESPONSE
    //    result["Status"] = "Failure";
    //    result["ErrorMessage"] = err_msg;
    //    return result;
    //}

    //GET CDC VARIABLES
    sql_stmt = `
               SELECT TASK_KEY
               , SYS_CHANGE_VERSION_START
               , CDC_MIN_DATE
               FROM ` + AUDIT_DB + `.AUDIT.ETL_TASK
               WHERE TASK_KEY = (
                           SELECT MAX(TASK_KEY)
                           FROM ` + AUDIT_DB + `.AUDIT.ETL_TASK
                           WHERE PRIMARY_SOURCE_DATABASE_NAME = '` + SOURCE_DB + `'
                           AND PRIMARY_SOURCE_SCHEMA_NAME =  '` + SOURCE_SCHEMA + `'
                           AND PRIMARY_SOURCE_TABLE_NAME = '` + SOURCE_TBL + `'
                           );`;
    sql_stmt = snowflake.createStatement({sqlText: sql_stmt});
    //try {
        cdc_var = sql_stmt.execute();
        cdc_var.next();
        task_key = cdc_var.getColumnValue(1);
        sys_change_version_start = cdc_var.getColumnValue(2);

        //CHECK FULL LOAD IND TO DETERMINE CDC DATE
        if (FULL_LOAD_IND = 'N') {
            cdc_min_date = cdc_var.getColumnValue(3);
        }
        else {
            cdc_min_date = '1900-01-01';
        }

        //APPEND RESULTS TO JSON RESPONSE
        result["TaskKey"] = task_key;
        result["CDC_MIN_DATE"] = cdc_min_date;
        result["SYS_CHANGE_VERSION_START"] = sys_change_version_start;
        return result;

    //} catch(err) {
    //    err_msg =  `Failed to get CDC Information: Code: ` + err.code + `\n  State: ` + err.state;
    //    err_msg += `\n  Message: ` + err.message;
    //    err_msg += `\nStack Trace:\n` + err.stackTraceTxt;
    //
    //    //APPEND ERROR TO JSON RESPONSE
    //    result["Status"] = "Failure";
    //    result["ErrorMessage"] = err_msg;
    //    return result;
    //}

    //UPDATE JSON RESPONSE TO SHOW SUCCESS
    result["Status"] = "Success";
    return result;

$$;
