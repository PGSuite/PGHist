## PGHist | History of table changes in PostgreSQL

### [Download](https://pghist.org/en/download/) ###
### [Documentation](https://pghist.org/en/documentation/) ### 
### [Example](https://pghist.org/en/#example-simple) ### 

### Description ###

Tool PGHIST keeps history of table changes and allows to get log(audit) of changes by row, list of changes by field indicating user,
time of the change, transaction, other technical information and table as of date-time in the past (versioning).
To display information in user interface, SQL expressions are defined to describe changed table rows and fields.


### Main functions and view ###
  
*   **pghist.hist\_enable(\[schema\],\[table\])** - enable history keeping 

*   **\[schema\].\[table\]_hist** - log(audit) of changes by row, optimized for analysis
  
*   **\[schema\].\[table\]\_changes** - list of changes by field, optimized for display to the user

*   **\[schema\].\[table\]\_at\_timestamp** - table at date-time in the past (versioning)
  

### Important qualities ### 

*   **Storage optimization** - saving only old values of changed fields and primary key. The Transaction-Expression-Row storage structure matches the operation of the DBMS and minimizes redundancy.
*   **Versatility** - possible to get change log, list of only changed data from several tables and table at a point in time.
*   **Descriptions** - for each table and its columns, it is possible to define SQL expressions to describe changed rows and field values. By default, descriptions are created for foreign key fields and table rows
*   **Inheritance** - stored procedures have parameter "cascade" that allows you to get data with or without inheritance
*   **Transaction and SQL statements** - changes have a reference to SQL statements, that references a transaction. Can get all the changes made within a single transaction or rows within one expression
*   **Indexes** - for a history table, an index is built on the primary key column(s), for a table at a point in time - standard indexes on columns
*   **Condition (optional)** - when getting a list of changes, you can specify a condition with or without parameter
*   **Autocorrection** - when performing DDL operations on a table (alter table, create index, etc.), a trigger fires, that corrects the history keeping. When a table is deleted, its history is also deleted
