## PGHist | Changes history and tables audit in PostgreSQL
  
### Description ###

Tool keeps history of table changes and allows to get table as of date-time in the past, list of changes by fields indicating user, time of the change, transaction and other technical information.  


### Main functions ###
  
*   **pghist.hist\_enable(\[schema\],\[table\])** - enable history keeping 
  
*   **\[schema\].\[table\]\_at\_timestamp** - table at date-time in the past
  
*   **\[schema\].\[table\]\_changes** - list of changes 
  
*   **pghist.hist\_disable(\[schema\],\[table\])** - disable history keeping


### Important qualities ### 

*   **Versatility** - possible to get table at a point in time and list of only changed data, access to the operation history table, union (union all) changes in table table into one select
*   **Inheritance** - when tables are inherited, their history is also inherited. Stored procedures have parameter "cascade" that allows you to get data with or without inheritance
*   **Single transaction** - changes have a reference to the transaction. You can get all the changes made within a single transaction
*   **Indexes** - for a table storing history, indexes are automatically built similarly to the main table. SQL queries to the history table are executed using an index, for example, by the ID field
*   **Condition (optional)** - when getting a list of changes, you can specify a condition with or without parameter
*   **Autocorrection** - when performing DDL operations on a table (alter table, create index, etc.), a trigger fires, that corrects the history keeping. When a table is deleted, its history is also deleted, when history is enabled current table values transferred to history

### [Documentation](http://pghist.org/en/documentation/) ### 
### [Example](http://pghist.org/en/#example) ### 
### [Download](http://pghist.org/en/download/) ###

