Please provide Python code that can do the following:
I have a spreadsheet with about 10 columns.  There is a header on the third row of the spreadsheet and the data is below the header.
I want to use DLT (Data Load Tool) with pandas (engine=openpyxl) to load the data into a Postgres database table with the same name as the spreadsheet file (“c01b.xlsx”).  The database should be named “comms” and the table should be in a schema named “bronze”.  
The path to the spreadsheet is “../data/xlsx/c01b.xlsx”.  The spreadsheet contains only one sheet named “c01b”.  
I want all the columns in the Postgres table are to be of type text.  
I want the program to add a new column named “bronze_row_uuid” and populate each row with a UUID.
