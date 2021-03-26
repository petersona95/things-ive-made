/*Generic Email Message with Column queries*/
/*email output is html table with sql data*/
/*used on sql server*/
DECLARE
     @tableHTML NVARCHAR(MAX)
    ,@SubjectLine NVARCHAR(255) 
    ,@ToLine NVARCHAR(255)
    ,@CCLine NVARCHAR(255)
                
SET @ToLine = 'receipient@company.com' 
SET @CCLine = ''
SET @SubjectLine = 'Fuzzy Match Job Completed (' + CONVERT(NVARCHAR(100), @@SERVERNAME) + ')'

BEGIN  
SET @tableHTML = 
    N'<h2><font face="Calibri">MTR Fuzzy Match Client List – </font></h2>' + 
    N'<table border="1" rules="none" cellpadding="6" cellspacing="-1"><font face="Calibri" size=2>' + 
    N'<tr>' +
    N'<th bgcolor="#C5D9F1">Client</th>' + 
    N'<th bgcolor="#C5D9F1">Fileset</th>' + 
    N'</tr>' + 
    CAST
    (
        (
            SELECT
                td = cl.ClientName,'', 
                td = cl.FilesetName,''
			FROM FuzzyMatch.ClientsLoaded cl
			ORDER BY cl.ClientName, cl.FilesetName
        FOR XML PATH('tr'), TYPE
        ) AS NVARCHAR(MAX)
    ) + N'</font></table>'  
END  

BEGIN
	EXEC msdb.dbo.sp_send_dbmail 
		@profile_name = 'Production Notifications Profile', 
		@recipients = @ToLine, 
		@copy_recipients = @CCLine, 
		@subject = @SubjectLine,
		@body = @tableHTML, 
		@body_format = 'HTML'
END