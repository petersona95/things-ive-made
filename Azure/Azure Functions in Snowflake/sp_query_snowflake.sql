call APT_DW.AUDIT.SP_QUERY_SNOWFLAKE(
    'INSERT INTO APT_DW.GA.USER_TEST VALUES(''123456'',''Alex Peterson'',''alex.peterson@apt'',''22'',current_timestamp(),current_user(),''22'',current_timestamp(),current_user(),false)'
    )

CREATE OR REPLACE PROCEDURE APT_DW.AUDIT.SP_QUERY_SNOWFLAKE(QUERY VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    //INITIALIZE THE RETURN IN CASE OF FAILURES
    result = {
            "Status" : "Unknown",
            "ErrorMessage" : "None"
            }

    //GET INITIAL ROW COUNT FROM THE TABLE
    sql_stmt = QUERY
    sql_stmt = snowflake.createStatement({sqlText: sql_stmt});
    try {
        qry_rslt = sql_stmt.execute();
        qry_rslt.next();
    } catch(err) {
        err_msg =  `Query Execution Failed: Code: ` + err.code + `\n  State: ` + err.state;
        err_msg += `\n  Message: ` + err.message;
        err_msg += `\nStack Trace:\n` + err.stackTraceTxt;
        throw err_msg;
    }

    //APPEND SUCCESS JSON RESPONSE
    result["Status"] = "Success";
    return result;
$$;