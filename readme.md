## PGHist | History of table changes in PostgreSQL

### [Download](https://pghist.org/en/download/) ###
### [Documentation](https://pghist.org/en/documentation/) ### 
### [Example](https://pghist.org/en/#example-simple) ### 

### Description ###

Tool PGHIST keeps history of table changes and allows to get log(audit) of changes by row, list of changes by field indicating user,
time of the change, transaction, other technical information and table as of date-time in the past (versioning).
To display information in user interface, SQL expressions are defined to describe changed table rows and fields.


### Main functions and view###
  
*   **pghist.hist\_enable(\[schema\],\[table\])** - enable history keeping 

*   **\[schema\].\[table\]_hist** - log(audit) of changes by row, optimized for analysis
  
*   **\[schema\].\[table\]\_changes** - list of changes by field, optimized for display to the user

*   **\[schema\].\[table\]\_at\_timestamp** - table at date-time in the past (versioning)

pghist.hist_enable([schema],[table])	-	enable history keeping
[schema].[table]_hist	-	log(audit) of changes by row, optimized for analysis
[schema].[table]_changes	-	list of changes by field, optimized for display to the user
[schema].[table]_at_timestamp	-	table at date-time in the past (versioning)

  

### Important qualities ### 

*   **Versatility** - possible to get table at a point in time and list of only changed data, access to the operation history table, union (union all) changes in table table into one select
*   **Descriptions** - for each table and its columns, it is possible to define SQL expressions to describe changed rows and field values. By default, descriptions are created for foreign key fields and table rows
*   **Inheritance** - when tables are inherited, their history is also inherited. Stored procedures have parameter "cascade" that allows you to get data with or without inheritance
*   **Single transaction** - changes have a reference to the transaction. You can get all the changes made within a single transaction
*   **Indexes** - for a table storing history, indexes are automatically built similarly to the main table. SQL queries to the history table are executed using an index, for example, by the ID field
*   **Condition (optional)** - when getting a list of changes, you can specify a condition with or without parameter
*   **Autocorrection** - when performing DDL operations on a table (alter table, create index, etc.), a trigger fires, that corrects the history keeping. When a table is deleted, its history is also deleted, when history is enabled current table values transferred to history
