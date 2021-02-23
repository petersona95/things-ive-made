USE ROLE SYSADMIN_INTERNAL_DW;
use warehouse DEMO_WH;
call APT_DW.AUDIT.SP_END_AUDIT_TASK(
    'APT_DW'
    , 'GA'
    , 'USER_TEST'
    , '7'
    , 'insert into user'
    , 'SOURCE_TYPE'
    );

SELECT * FROM APT_DW.AUDIT.ETL_TASK;
SELECT * FROM APT_DW.GA.USER_TEST ORDER BY USER_ID DESC;

--UPDATE ROWS SO WE CAN TEST THE END AUDIT TASK
UPDATE APT_DW.GA.USER_TEST
SET ETL_TASK_KEY = 7
WHERE USER_ID >= 792733

INSERT INTO APT_DW.GA.USER_TEST VALUES(
                                    1234567,
                                    'Alex Peterson',
                                    'alex.peterson@apt',
                                    '7',
                                    current_timestamp(),
                                    current_user(),
                                    '7',
                                    current_timestamp(),
                                    current_user(),
                                    false
                                      )

INSERT INTO APT_DW.GA.USER_TEST VALUES('123456','Alex Peterson','alex.peterson@apt','22',current_timestamp(),current_user(),'22',current_timestamp(),current_user(),false)

CREATE OR REPLACE PROCEDURE APT_DW.AUDIT.SP_END_AUDIT_TASK(DEST_DB VARCHAR, DEST_SCHEMA VARCHAR
, DEST_TBL VARCHAR, TASK_KEY VARCHAR, TASK_NAME VARCHAR, SOURCE_TYPE VARCHAR)
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

    //QUERIES TO DETERMINE EXTRACT ROW COUNT AND FINAL ROW COUNT
    //IF SOURCE_TYPE = MASTER, MARK THEM AS 0
    if (SOURCE_TYPE === "Master") {
        extract = `SELECT 0 AS EXTRACT_ROW_COUNT`;
        final = `SELECT 0 AS TARGET_FINAL_ROW_COUNT`;
        max_version = `SELECT 0 AS MAX_CHANGE_VERSION`;
    }
    else {
        extract = `SELECT COUNT(1) AS EXTRACT_ROW_COUNT FROM  ` + DEST_DB + `.` + DEST_SCHEMA + `.` + DEST_TBL + ` WHERE ETL_TASK_KEY = ` + TASK_KEY;
        final = `SELECT COUNT(1) AS TARGET_FINAL_ROW_COUNT FROM  ` + DEST_DB + `.` + DEST_SCHEMA + `.` + DEST_TBL;
        max_version = `SELECT 0 AS MAX_CHANGE_VERSION`;
    }

    //UPDATE THE AUDIT TABLE
    sql_stmt = `
               UPDATE AUDIT.ETL_TASK 
               SET EXECUTION_STOP_DATE = CURRENT_timestamp()::TIMESTAMP_NTZ(9)
               , SYS_CHANGE_VERSION_END = COALESCE(ctes.MAX_CHANGE_VERSION, 0)
               , EXTRACT_ROW_COUNT = ctes.EXTRACT_ROW_COUNT
               , TARGET_FINAL_ROW_COUNT = ctes.TARGET_FINAL_ROW_COUNT
               , SUCCESSFUL_PROCESSING_IND = 1
               FROM (
                        WITH
                        EXTRACT AS ( ` + extract + `)
                        , FINAL AS ( ` + final + `)
                        , MAX_VERSION AS ( ` + max_version + `)
                        SELECT
                          EXTRACT_ROW_COUNT
                        , TARGET_FINAL_ROW_COUNT
                        , MAX_CHANGE_VERSION
                        FROM FINAL, EXTRACT, MAX_VERSION
                        ) ctes
               WHERE TASK_KEY =  '` + TASK_KEY + `'`;
    sql_stmt = snowflake.createStatement({sqlText: sql_stmt});
    //try {
        exec_stmt = sql_stmt.execute();
    //} catch(err) {
    //    err_msg =  `Failed to Update Audit Table: Code: ` + err.code + `\n  State: ` + err.state;
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