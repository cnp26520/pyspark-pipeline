// Test for valid Data
USE PYSPARK_EXAMPLE.VALID_DATA;

CREATE STAGE valid_data_stage 
	URL = 'azure://snowloader.blob.core.windows.net/valid' 
	CREDENTIALS = ( AZURE_SAS_TOKEN = '?sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2023-12-14T14:21:02Z&st=2023-12-14T06:21:02Z&spr=https&sig=CsG1oDalHHjue6ZrDQgFro8%2BbydXdgG73O56C%2F%2BoczA%3D' ) 
	DIRECTORY = ( ENABLE = true );

list @PYSPARK_EXAMPLE.VALID_DATA.VALID_DATA_STAGE;

create or replace table valid_data_table(
emp_id varchar(36),
employee_name varchar(20),
department varchar(15),
state varchar(4),
age int ,
salary int,
bonus int,
nulls BOOLEAN);

COPY INTO valid_data_table
  FROM @PYSPARK_EXAMPLE.VALID_DATA.VALID_DATA_STAGE
  PATTERN='.*part.*.csv'
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
  purge=TRUE;

  select * FROM valid_data_table;

  
  Select * FROM valid_data_table
  WHERE employee_name IS  NULL
  OR emp_id IS NULL
  OR DEPARTMENT IS NULL
  OR STATE IS NULL
  OR SALARY IS NULL
  OR AGE IS NULL
  OR BONUS IS NULL
  OR nulls IS NULL;



  //invalid data portion

  USE PYSPARK_EXAMPLE.INVALID_DATA;
  
  CREATE STAGE invalid_data_stage 
	URL = 'azure://snowloader.blob.core.windows.net/invalid' 
	CREDENTIALS = ( AZURE_SAS_TOKEN = '?sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2023-12-14T14:21:02Z&st=2023-12-14T06:21:02Z&spr=https&sig=CsG1oDalHHjue6ZrDQgFro8%2BbydXdgG73O56C%2F%2BoczA%3D' ) 
	DIRECTORY = ( ENABLE = true );

    list @PYSPARK_EXAMPLE.INVALID_DATA.INVALID_DATA_STAGE;

    create or replace table invalid_data_table(
    emp_id varchar(36),
    employee_name varchar(20),
    department varchar(15),
    state varchar(4),
    age int ,
    salary int,
    bonus int,
    nulls BOOLEAN)
    ;

    COPY INTO invalid_data_table
    FROM @PYSPARK_EXAMPLE.INVALID_DATA.INVALID_DATA_STAGE
    PATTERN='.*part.*.csv'
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',')
    purge=TRUE;


// Test for Invalid Data

SELECT * FROM invalid_data_table;
    
SELECT * FROM INVALID_DATA_TABLE
WHERE employee_name IS NOT NULL
AND emp_id IS NOT NULL
AND DEPARTMENT IS NOT NULL
AND STATE IS NOT NULL
AND SALARY IS NOT NULL
AND AGE IS NOT NULL
AND BONUS IS NOT NULL
AND nulls IS NOT NULL;