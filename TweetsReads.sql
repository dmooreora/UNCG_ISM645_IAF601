SET ANSI_NULLS ON 
GO 
SET QUOTED_IDENTIFIER ON 
GO 

USE [sentimentdb]
GO
 
/****** Object:  Table [dbo].[Hierarchy]    Script Date: 10/3/2020 7:23:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Hierarchy](
	[element_id] [int] IDENTITY(1,1) NOT NULL,
	[sequenceNo] [varchar](10) NULL,
	[parent_ID] [varchar](10) NULL,
	[Object_ID] [varchar](10) NULL,
	[NAME] [nvarchar](2000) NULL,
	[StringValue] [nvarchar](max) NULL,
	[ValueType] [varchar](10) NULL,
PRIMARY KEY CLUSTERED 
(
	[element_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO





TRUNCATE TABLE HIERARCHY;

CREATE PROC [dbo].[ns_txt_file_read]  
    @os_file_name NVARCHAR(256) 
   ,@text_file VARCHAR(MAX) OUTPUT  
/* Reads a text file into @text_file 
* 
* Transactions: may be in a transaction but is not affected 
* by the transaction. 
* 
* Error Handling: Errors are not trapped and are thrown to 
* the caller. 
* 
* Example: 
    declare @t varchar(max) 
    exec ns_txt_file_read 'c:\temp\SampleTextDoc.txt', @t output 
    select @t as [SampleTextDoc.txt] 
* 
* History: 
* WHEN       WHO        WHAT 
* ---------- ---------- --------------------------------------- 
* 2007-02-06 anovick    Initial coding 
**************************************************************/  
AS  
DECLARE @sql NVARCHAR(MAX) 
       ,@parmsdeclare NVARCHAR(4000)  

SET NOCOUNT ON  


SET @sql = 'select @text_file=(select * from openrowset ( 
           bulk ''' + @os_file_name + ''' 
           ,SINGLE_CLOB) rs 
           )' 

SET @parmsdeclare = '@text_file varchar(max) OUTPUT'  

EXEC sp_executesql @stmt = @sql 
                 , @params = @parmsdeclare 
                 , @text_file = @text_file OUTPUT 

DECLARE @t VARCHAR(MAX)
begin 
  EXEC ns_txt_file_read 'c:\tweets\tweets_2020_09_28_1_test.json', @t output 
   
   INSERT INTO Hierarchy
   Select sequenceNo, parent_ID,Object_ID, Name, StringValue, ValueType from parseJSON(@t);
end
go


select * from openrowset(bulk 'c:\tweets\tweets_2020_09_28_1_test.json', SINGLE_CLOB) rs;


 DECLARE
	    @FirstObject INT, --the index of the first open bracket found in the JSON string
	    @OpenDelimiter INT,--the index of the next open bracket found in the JSON string
	    @NextOpenDelimiter INT,--the index of subsequent open bracket found in the JSON string
	    @NextCloseDelimiter INT,--the index of subsequent close bracket found in the JSON string
	    @Type NVARCHAR(10),--whether it denotes an object or an array
	    @NextCloseDelimiterChar CHAR(1),--either a '}' or a ']'
	    @Contents NVARCHAR(MAX), --the unparsed contents of the bracketed expression
	    @Start INT, --index of the start of the token that you are parsing
	    @end INT,--index of the end of the token that you are parsing
	    @param INT,--the parameter at the end of the next Object/Array token
	    @EndOfName INT,--the index of the start of the parameter at end of Object/Array token
	    @token NVARCHAR(200),--either a string or object
	    @value NVARCHAR(MAX), -- the value as a string
	    @SequenceNo INT, -- the sequence number within a list
	    @Name NVARCHAR(200), --the Name as a string
	    @Parent_ID INT,--the next parent ID to allocate
	    @lenJSON INT,--the current length of the JSON String
	    @characters NCHAR(36),--used to convert hex to decimal
	    @result BIGINT,--the value of the hex symbol being parsed
	    @index SMALLINT,--used for parsing the hex value
	    @Escape INT, --the index of the next escape character
		 @JSON NVARCHAR(MAX)
	    
	  DECLARE @Strings TABLE /* in this temporary table we keep all strings, even the Names of the elements, since they are 'escaped' in a different way, and may contain, unescaped, brackets denoting objects or lists. These are replaced in the JSON string by tokens representing the string */
	    (
	     String_ID INT IDENTITY(1, 1),
	     StringValue NVARCHAR(MAX)
	    )
BEGIN
	  SELECT--initialise the characters to convert hex to ascii
	    @characters='0123456789abcdefghijklmnopqrstuvwxyz',
	    @SequenceNo=0, --set the sequence no. to something sensible.
	  /* firstly we process all strings. This is done because [{} and ] aren't escaped in strings, which complicates an iterative parse. */
	    @Parent_ID=0;

        EXEC ns_txt_file_read 'c:\tweets\tweets_2020_09_28_1_test.json', @json output 

	  WHILE 1=1 --forever until there is nothing more to do
	    BEGIN
		      SELECT
	        @start=TRY_CONVERT(int,PATINDEX('%[^a-zA-Z]["]%', @json collate SQL_Latin1_General_CP850_Bin));--next delimited string
	      IF @start=0 BREAK --no more so drop through the WHILE loop
	      IF SUBSTRING(@json, @start+1, 1)='"' 
	        BEGIN --Delimited Name
	          SET @start=@Start+1;
	          SET @end=TRY_CONVERT(int,PATINDEX('%[^\]["]%', RIGHT(@json, LEN(@json+'|')-@start) collate SQL_Latin1_General_CP850_Bin));
	        END
	      IF @end=0 --either the end or no end delimiter to last string
	        BEGIN-- check if ending with a double slash...
             SET @end=TRY_CONVERT(int,PATINDEX('%[\][\]["]%', RIGHT(@json, LEN(@json+'|')-@start) collate SQL_Latin1_General_CP850_Bin));
 		     IF @end=0 --we really have reached the end 
				BEGIN
				BREAK --assume all tokens found
				END
			END 
	      SELECT @token=SUBSTRING(@json, @start+1, @end-1)
	      --now put in the escaped control characters
	      SELECT @token=REPLACE(@token, FromString, ToString)
	      FROM
	        (SELECT           '\b', CHAR(08)
	         UNION ALL SELECT '\f', CHAR(12)
	         UNION ALL SELECT '\n', CHAR(10)
	         UNION ALL SELECT '\r', CHAR(13)
	         UNION ALL SELECT '\t', CHAR(09)
			 UNION ALL SELECT '\"', '"'
	         UNION ALL SELECT '\/', '/'
	        ) substitutions(FromString, ToString)
		SELECT @token=Replace(@token, '\\', '\')
	      SELECT @result=0, @escape=1
	  --Begin to take out any hex escape codes
	      WHILE @escape>0
	        BEGIN
	          SELECT @index=0,
	          --find the next hex escape sequenceR
	          @escape=TRY_CONVERT(int,PATINDEX('%\x[0-9a-f][0-9a-f][0-9a-f][0-9a-f]%', @token collate SQL_Latin1_General_CP850_Bin))
	          IF @escape>0 --if there is one
	            BEGIN
	              WHILE @index<4 --there are always four digits to a \x sequence   
	                BEGIN
	                  SELECT --determine its value
	                    @result=@result+POWER(16, @index)
	                    *(CHARINDEX(SUBSTRING(@token, @escape+2+3-@index, 1),
	                                @characters)-1), @index=@index+1 ;
	         
	                END
	                -- and replace the hex sequence by its unicode value
	              SELECT @token=STUFF(@token, @escape, 6, NCHAR(@result))
	            END
	        END
	      --now store the string away 
	      INSERT INTO @Strings (StringValue) SELECT @token
	      -- and replace the string with a token
	      SELECT @JSON=STUFF(@json, @start, @end+1,
	                    '@string'+CONVERT(NCHAR(5), @@identity))
	    END
	  -- all strings are now removed. Now we find the first leaf.  
	  WHILE 1=1  --forever until there is nothing more to do
	  BEGIN
	 
	  SELECT @Parent_ID=@Parent_ID+1
	  --find the first object or list by looking for the open bracket
	  SELECT @FirstObject=TRY_CONVERT(INT,PATINDEX('%[{[[]%', @json collate SQL_Latin1_General_CP850_Bin))--object or array
	  IF @FirstObject = 0 BREAK
	  IF (SUBSTRING(@json, @FirstObject, 1)='{') 
	    SELECT @NextCloseDelimiterChar='}', @type='object'
	  ELSE 
	    SELECT @NextCloseDelimiterChar=']', @type='array'
	  SELECT @OpenDelimiter=@firstObject
	  WHILE 1=1 --find the innermost object or list...
	    BEGIN
	 
	      SELECT
	        @lenJSON=LEN(@JSON+'|')-1
	  --find the matching close-delimiter proceeding after the open-delimiter
	      SELECT
	        @NextCloseDelimiter=CHARINDEX(@NextCloseDelimiterChar, @json,
	                                      @OpenDelimiter+1)
	  --is there an intervening open-delimiter of either type
	      SELECT @NextOpenDelimiter=PATINDEX('%[{[[]%',
	             RIGHT(@json, @lenJSON-@OpenDelimiter)collate SQL_Latin1_General_CP850_Bin)--object
	      IF @NextOpenDelimiter=0 
	        BREAK
	      SELECT @NextOpenDelimiter=@NextOpenDelimiter+@OpenDelimiter
	      IF @NextCloseDelimiter<@NextOpenDelimiter 
	        BREAK
	      IF SUBSTRING(@json, @NextOpenDelimiter, 1)='{' 
	        SELECT @NextCloseDelimiterChar='}', @type='object'
	      ELSE 
	        SELECT @NextCloseDelimiterChar=']', @type='array'
	      SELECT @OpenDelimiter=@NextOpenDelimiter
	    END
	  ---and parse out the list or Name/value pairs
	  SELECT
	    @contents=SUBSTRING(@json, @OpenDelimiter+1,
	                        @NextCloseDelimiter-@OpenDelimiter-1)
	  SELECT
	    @JSON=STUFF(@json, @OpenDelimiter,
	                @NextCloseDelimiter-@OpenDelimiter+1,
	                '@'+@type+CONVERT(NCHAR(5), @Parent_ID))
	  WHILE (PATINDEX('%[A-Za-z0-9@+.e]%', @contents collate SQL_Latin1_General_CP850_Bin))<>0 
	    BEGIN
	      IF @Type='object' --it will be a 0-n list containing a string followed by a string, number,boolean, or null
	        BEGIN
	          SELECT
	            @SequenceNo=0,@end=CHARINDEX(':', ' '+@contents)--if there is anything, it will be a string-based Name.
	          SELECT  @start=TRY_CONVERT(INT,PATINDEX('%[^A-Za-z@][@]%', ' '+@contents collate SQL_Latin1_General_CP850_Bin))--AAAAAAAA
              SELECT @token=RTrim(Substring(' '+@contents, @start+1, @End-@Start-1)),
	            @endofName=TRY_CONVERT(INT,PATINDEX('%[0-9]%', @token collate SQL_Latin1_General_CP850_Bin)),
	            @param=RIGHT(@token, LEN(@token)-@endofName+1)
	          SELECT
	            @token=LEFT(@token, @endofName-1),
	            @Contents=RIGHT(' '+@contents, LEN(' '+@contents+'|')-@end-1)
	          SELECT  @Name=StringValue FROM @strings
	            WHERE string_id=@param --fetch the Name
	        END
	      ELSE 
	        SELECT @Name=null,@SequenceNo=@SequenceNo+1 
	      SELECT
	        @end=CHARINDEX(',', @contents)-- a string-token, object-token, list-token, number,boolean, or null
                IF @end=0
	        --HR Engineering notation bugfix start
	          IF ISNUMERIC(@contents) = 1
		    SELECT @end = LEN(@contents) + 1
	          Else
	        --HR Engineering notation bugfix end 
		  SELECT  @end=TRY_CONVERT(INT,PATINDEX('%[A-Za-z0-9@+.e][^A-Za-z0-9@+.e]%', @contents+' ' collate SQL_Latin1_General_CP850_Bin)) + 1
	       SELECT
	        @start=TRY_CONVERT(INT,PATINDEX('%[^A-Za-z0-9@+.e][A-Za-z0-9@+.e]%', ' '+@contents collate SQL_Latin1_General_CP850_Bin))
	      --select @start,@end, LEN(@contents+'|'), @contents  
	      SELECT
	        @Value=RTRIM(SUBSTRING(@contents, @start, @End-@Start)),
	        @Contents=RIGHT(@contents+' ', LEN(@contents+'|')-@end)
	      IF SUBSTRING(@value, 1, 7)='@object' 
	        INSERT INTO hierarchy
	          (Name, SequenceNo, Parent_ID, StringValue, Object_ID, ValueType)
	          SELECT @Name, CONVERT(NCHAR(5), @SequenceNo),CONVERT(NCHAR(5), @Parent_ID), SUBSTRING(@value, 8, 5),
	            SUBSTRING(@value, 8, 5), 'object' 
	      ELSE 
	        IF SUBSTRING(@value, 1, 6)='@array' 
	          INSERT INTO hierarchy
	            (Name, SequenceNo, Parent_ID, StringValue, Object_ID, ValueType)
	            SELECT @Name, CONVERT(NCHAR(5), @SequenceNo), CONVERT(NCHAR(5), @Parent_ID), SUBSTRING(@value, 7, 5),
	              SUBSTRING(@value, 7, 5), 'array' 
	        ELSE 
	          IF SUBSTRING(@value, 1, 7)='@string' 
	            INSERT INTO hierarchy
	              (Name, SequenceNo, Parent_ID, StringValue, ValueType)
	              SELECT @Name, CONVERT(NCHAR(5), @SequenceNo),CONVERT(NCHAR(5), @Parent_ID), StringValue, 'string'
	              FROM @strings
	              WHERE string_id=SUBSTRING(@value, 8, 5)
	          ELSE 
	            IF @value IN ('true', 'false') 
	              INSERT INTO hierarchy
	                (Name, SequenceNo, Parent_ID, StringValue, ValueType)
	                SELECT @Name, CONVERT(NCHAR(5), @SequenceNo), CONVERT(NCHAR(5), @Parent_ID), @value, 'boolean'
	            ELSE
	              IF @value='null' 
	                INSERT INTO hierarchy
	                  (Name, SequenceNo, Parent_ID, StringValue, ValueType)
	                  SELECT @Name, CONVERT(NCHAR(5), @SequenceNo), CONVERT(NCHAR(5), @Parent_ID), @value, 'null'
	              ELSE
	                IF PATINDEX('%[^0-9]%', @value collate SQL_Latin1_General_CP850_Bin)>0 
	                  INSERT INTO hierarchy
	                    (Name, SequenceNo, Parent_ID, StringValue, ValueType)
	                    SELECT @Name, CONVERT(NCHAR(5), @SequenceNo), CONVERT(NCHAR(5), @Parent_ID), @value, 'real'
	                ELSE
	                  INSERT INTO hierarchy
	                    (Name, SequenceNo, Parent_ID, StringValue, ValueType)
	                    SELECT @Name, CONVERT(NCHAR(5), @SequenceNo), CONVERT(NCHAR(5), @Parent_ID), @value, 'int'
	      if @Contents=' ' Select @SequenceNo=0
	    END
	   
	  END
	  
 
 
	INSERT INTO hierarchy (Name, SequenceNo, Parent_ID, StringValue, Object_ID, ValueType)
	  SELECT '-','1', NULL, '',CONVERT(NCHAR(5), @Parent_ID-1), @type
	--
	   RETURN
 
	END
GO

