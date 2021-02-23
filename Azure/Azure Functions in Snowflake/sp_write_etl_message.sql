SELECT * FROM APT_DW.AUDIT.ETL_MESSAGE_LOG

CALL APT_DW.AUDIT.SP_WRITE_ETL_MESSAGE(
    'APT_DW'
    , 'ABC123'
    , '7'
    , 'insert into user'
    , 'HEY! IT FAILED!'
)

CREATE OR REPLACE PROCEDURE APT_DW.AUDIT.SP_WRITE_ETL_MESSAGE(AUDIT_DB VARCHAR, PIPELINE_ID VARCHAR, TASK_KEY VARCHAR, TASK_NAME VARCHAR, MESSAGE VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$

    //INITIALIZE THE RETURN IN CASE OF FAILURES
    result = {
            "TaskName" : TASK_NAME,
            "TaskKey" : TASK_KEY,
            "Status" : "Unknown",
            "ErrorMessage" : "None"
            }

    // Step 1: Insert into audit table
    sql_stmt = `
                INSERT INTO ` + AUDIT_DB + `.AUDIT.ETL_MESSAGE_LOG (
                MESSAGE_LOG_KEY
                , PIPELINE_RUN_ID
                , TASK_KEY
                , TASK_NAME
                , MESSAGE_TYPE
                , MESSAGE_DATE
                , MESSAGE_LOCATION
                , MESSAGE
                )
                SELECT
                    ` + AUDIT_DB + `.AUDIT.SEQ_ETL_MESSAGE.nextval
                    , '` + PIPELINE_ID + `'
                    , '` + TASK_KEY + `'
                    , '` + TASK_NAME + `'
                    , 'FAILURE'
                    ,current_timestamp::timestamp_ntz
                    , 'ADF'
                    , '` + MESSAGE + `'`;
    sql_stmt = snowflake.createStatement({sqlText: sql_stmt});
    try {
        sql_exec = sql_stmt.execute();
        sql_exec.next();
    } catch(err) {
        err_msg =  `Failed to get insert error record: Code: ` + err.code + `\n  State: ` + err.state;
        err_msg += `\n  Message: ` + err.message;
        err_msg += `\nStack Trace:\n` + err.stackTraceTxt;

        //APPEND FAILURE TO JSON RESPONSE
        result["Status"] = "Failure";
        result["ErrorMessage"] = err_msg;
        return result;
    }

    //APPEND SUCCESS TO JSON RESPONSE
    result["Status"] = "Success";
    return result;

$$;