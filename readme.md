## PGHist | History of table changes in PostgreSQL

Tool `PGHist` keeps history of table changes and allows to get log(audit) of changes by row,
list of changes by field indicating user, time of the change, SQL query, transaction, other technical information
and table as of date-time in the past (versioning).
To display information in user interface, SQL expressions are defined to describe changed table rows and fields.
It is possible to override the operation name and username functions.

### Design and working principle ###

PGHIST is a schema with procedures and common tables: transactions, SQL expressions.
When history is enabled (procedure pghist.hist_enable), for specified table created additional table, triggers for insert,update,delete,truncate, stored procedures and view for obtaining data.
When a table is changed, triggers are fired that modify the history table.
There are also event triggers that rebuild the history table and recreate the stored procedures.

### Installation ###

The installer is [pghist_init.sql](https://github.com/PGHist/PGHist/raw/main/pghist_init.sql) file that creates `pghist` schema.  
The installation consists in executing it in the psql terminal client or SQL manager, for example:  

```bash
wget -O - -q https://github.com/PGHist/PGHist/raw/main/pghist_init.sql | psql -d [database]
```

Optional. If the developers are not superusers, need to grant them privileges on the pghist schema and its procedures.
To do this, use the SQL script [pghist_grants.sql](https://github.com/PGHist/PGHist/raw/main/pghist_grants.sql) with the roles variable. For example:

```bash
wget -O - -q https://github.com/PGHist/PGHist/raw/main/pghist_grants.sql | psql -d [database] -v roles=[developers]
```

### Extension ###

To install `PGHist` as an extension, unpack the [pghist_extension.tar](https://github.com/PGHist/PGHist/raw/main/extension/pghist_extension.tar) archive into the [sharedir]/extension directory of the postgres installation, for example (run as root):

```bash
wget -O - -q https://github.com/PGHist/PGHist/raw/main/extension/pghist_extension.tar | tar x -C `su - postgres -c "pg_config --sharedir"`/extension
```
More info on page [download](https://pghist.org/en/download/)

### Simple example ###

```sql
-- Create table
create table example(
  id int primary key,
  name varchar(20),
  number numeric(10,2),
  date date
);

-- Endable keeping history
call pghist.hist_enable('example');

-- Change table
insert into example values (1, 'Example', 10, current_date);
update example set number=20, date=date-1;

-- View change log by row
select * from example_hist;

-- View changes by field
select * from example_changes();

-- View table at timestamp 
select * from example_at_timestamp(now()-interval '10 second');
```

All examples in directory [example](https://github.com/PGHist/PGHist/tree/main/example)

### Main functions and view ###
  
*   **pghist.hist\_enable(\[schema\],\[table\])** - enable history keeping 

*   **\[schema\].\[table\]_hist** - log(audit) of changes by row, optimized for analysis
  
*   **\[schema\].\[table\]\_changes** - list of changes by field, optimized for display to the user

*   **\[schema\].\[table\]\_at\_timestamp** - table at date-time in the past (versioning)

Documentation in file [documentation/documentation.html](https://htmlpreview.github.io/?https://github.com/PGHist/PGHist/blob/main/documentation/documentation.html)  

### Important qualities ### 

*   **Storage optimization** - saving only old values of changed fields and primary key. The Transaction-Expression-Row storage structure matches the operation of the DBMS and minimizes redundancy.
*   **Versatility** - possible to get change log, list of only changed data from several tables and table at a point in time.
*   **Descriptions** - for each table and its columns, it is possible to define SQL expressions to describe changed rows and field values. By default, descriptions are created for foreign key fields and table rows
*   **Inheritance** - stored procedures have parameter "cascade" that allows you to get data with or without inheritance
*   **Transaction and SQL statements** - changes have a reference to SQL statements, that references a transaction. Can get all the changes made within a single transaction or rows within one expression
*   **Indexes** - for a history table, an index is built on the primary key column(s), for a table at a point in time - standard indexes on columns
*   **Condition (optional)** - when getting a list of changes, you can specify a condition with or without parameter
*   **Autocorrection** - when performing DDL operations on a table (alter table, create index, etc.), a trigger fires, that corrects the history keeping. When a table is deleted, its history is also deleted

Overview on site [pghist.org](https://pghist.org/)

### Support ### 

Of course you can create an issue, I will answer all requests.  
Also I will help to install and use the tool.  
Welcome to discussion !  

WhatsApp: [PGSuite (+7-936-1397626)](https://wa.me/79361397626)  
email: [support\@pgsuite.org](mailto:support@pgsuite.org?subject=PGXLS)

