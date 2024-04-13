create schema if not exists pghist;

create or replace function pghist.pghist_version() returns varchar language plpgsql as $$
begin
  return '24.1.5'; -- 2024.02.28 13:05:18
end; $$;

create table if not exists pghist.hist_transaction(
  id bigserial constraint hist_transaction_pkey primary key,
  xid bigint not null,  
  xact_start timestamptz not null,
  unique (xid, xact_start),
  application_name text,
  backend_pid integer not null,
  backend_start timestamptz not null,  
  db_user name not null,
  db_client_addr inet,
  db_client_hostname text,
  app_user name,
  app_client_addr text,
  app_client_hostname text  
);

create table if not exists pghist.hist_sql_log(
  id serial primary key,
  pghist_version varchar(8) not null default pghist.pghist_version(),
  transaction_id bigint not null references pghist.hist_transaction(id) on delete cascade,  
  schema name not null,
  table_name name not null,  
  sql_statement text not null
);

do $$ begin
  if to_regtype('pghist.table_change') is null then
    create type pghist.table_change as (
      column_name name,
      value_old text,
      value_old_desc text,      
      value_new text,
      value_new_desc text,
      row_desc text,      
      db_user varchar,
      app_user varchar,
      timestamp timestamptz,
      operation varchar,
      schema name,
      table_name name,
      table_comment text,     
      column_comment varchar,      
      column_pos int,
      row_pkey varchar[],
      transaction_id bigint,
      hist_id bigint
    );  
  end if;
end $$;

create sequence if not exists pghist.hist_id_seq;

create or replace function pghist.hist_transaction_id() returns bigint language plpgsql as $$
declare
  v_id bigint;
  v_id_text text;
begin
  v_id_text := coalesce(current_setting('pghist.transaction_id', true),'');
  if v_id_text!='' then
    return v_id_text::bigint;
  end if;  
  insert into pghist.hist_transaction(xid, xact_start, application_name, backend_pid, backend_start, db_user, db_client_addr, db_client_hostname, app_user, app_client_addr, app_client_hostname)
    select txid_current(), xact_start, application_name, pg_backend_pid(), backend_start, usename, client_addr, client_hostname, current_setting('app.user', true), current_setting('app.client_addr', true), current_setting('app.client_hostname', true)
    from pg_stat_activity p where pid = pg_backend_pid()
    returning id into v_id;
  v_id_text := set_config('pghist.transaction_id', v_id::text, true);
  return v_id;
end; $$;

create or replace procedure pghist.hist_execute_sql(schema name, table_name name, sql_statement varchar) language plpgsql security definer as $$
begin
  execute sql_statement;      
  insert into pghist.hist_sql_log(transaction_id,schema,table_name,sql_statement)
    values(pghist.hist_transaction_id(),schema,table_name,sql_statement);  
end; $$;

create or replace function pghist.hist_table_name(schema name, table_name name) returns varchar language plpgsql as $$
begin
  return 'hist_'||schema||'$'||table_name;
end; $$;

create or replace function pghist.hist_exists(schema name, table_name name) returns boolean language plpgsql as $$
begin
  return exists (select 1 from pg_tables where schemaname='pghist' and tablename=pghist.hist_table_name(schema,table_name));
end; $$;

create or replace function pghist.hist_column_desc_name(column_name name) returns varchar language plpgsql as $$
begin
  return lower(column_name)||'$hist_desc';
end; $$;

create or replace function pghist.hist_columns_to_text(columns name[], expr varchar) returns varchar language plpgsql as $$
declare
  v_text text;
begin
  execute format('select string_agg(%s, '','' order by pos) from unnest($1) with ordinality r(col,pos)', expr )
    into v_text
    using columns;	
  return v_text;  
end; $$;

create or replace procedure pghist.hist_object_names(schema name, table_name name, inout hist_schema name, inout hist_table_name name, inout trigger_iud_function_name name, inout trigger_iud_name name, inout trigger_truncate_function_name name, inout trigger_truncate_name name, inout view_hist_name name, inout table_changes_name name, inout table_at_timestamp_name name) language plpgsql as $$
begin
  hist_schema := 'pghist';	
  hist_table_name := pghist.hist_table_name(schema,table_name);
  --
  trigger_iud_function_name := schema||'.'||hist_schema||'_'||table_name||'_fn_iud';
  trigger_iud_name := hist_schema||'_'||table_name||'_tg_iud';
  trigger_truncate_function_name := schema||'.'||hist_schema||'_'||table_name||'_fn_truncate';
  trigger_truncate_name := hist_schema||'_'||table_name||'_tg_truncate';
  --
  view_hist_name := schema||'.'||table_name||'_hist';  
  table_changes_name := schema||'.'||table_name||'_changes';
  table_at_timestamp_name := schema||'.'||table_name||'_at_timestamp';
end; $$;

create or replace procedure pghist.hist_enable(schema name, table_name name) language plpgsql as $body$
declare 
  v_schema name := lower(schema);
  v_table_name name := lower(table_name);
  v_table_oid oid := (v_schema||'.'||v_table_name)::regclass::oid;
  v_table_owner name;
  v_table_row_desc_expr text; 
  v_children varchar[][];
  v_columns_name name[];
  v_columns_type name[];
  v_columns_pos int[];  
  v_columns_comment text[];
  v_columns_hist_desc name[];
  v_columns_pkey name[];
  --
  v_hist_schema name;  
  v_hist_table_name name;
  v_hist_table_fix_columns text := 'hist_id,hist_transaction_id,hist_operation,hist_state,hist_pkey,hist_row_desc,';
  v_hist_table_fix_values text := 'v_hist_id,v_transaction_id,tg_op,'; 
  v_hist_table_fix_column_count int := 6; 
  v_hist_table_oid oid;
  v_hist_columns_ddl text[]; 
  v_hist_columns_ddl_desc text[];
  v_hist_columns_name text[];
  v_hist_row_values text[];
  v_hist_row_values_pkey text;
  --
  v_trigger_iud_function_name name;
  v_trigger_iud_name name;
  v_trigger_truncate_function_name name;
  v_trigger_truncate_name name;
  --
  v_view_hist_name name;  
  v_table_changes_name name;
  v_table_at_timestamp_name name;
  --
  v_sql text; 
  v_newline char := chr(10);
  v_type_convert text; 
  rec record;
begin
  select relowner::regrole::name into v_table_owner from pg_class where oid=v_table_oid;
  call pghist.hist_object_names(v_schema, v_table_name, v_hist_schema, v_hist_table_name, v_trigger_iud_function_name, v_trigger_iud_name, v_trigger_truncate_function_name, v_trigger_truncate_name, v_view_hist_name, v_table_changes_name, v_table_at_timestamp_name);  
  select array_agg(attname),
         array_agg(typname),
         array_agg(column_pos),         
         array_agg(quote_nullable(column_comment))
    into v_columns_name,
         v_columns_type,
         v_columns_pos,         
         v_columns_comment
    from ( 
      select attname, typname, row_number() over (order by attnum) column_pos, col_description(attrelid,attnum) column_comment    
        from pg_attribute a
        join pg_type t on t.oid=a.atttypid 
        where attrelid=v_table_oid and attnum>0 and not attisdropped
        order by attnum
    ) a;
  select coalesce(array_agg(attname order by col_pos),array[]::name[])
    into v_columns_pkey
    from unnest( (select conkey from pg_constraint where conrelid=v_table_oid and contype='p') ) with ordinality c(col_num,col_pos)
    join pg_attribute a on attrelid=v_table_oid and attnum = col_num;
  if array_length(v_columns_pkey,1) is not null then
    v_hist_row_values_pkey := 'array['||pghist.hist_columns_to_text(v_columns_pkey, '''row.''||col||''::varchar''')||']';
  else     
     v_hist_row_values_pkey := 'null';
  end if;  
  select array_agg(array[c.relnamespace::regnamespace::name,c.relname] order by c.relnamespace::regnamespace::name,c.relname)
    into v_children
    from pg_inherits i
    join pg_class c on c.oid=i.inhrelid
    where inhparent = v_table_oid;
  --
  select oid into v_hist_table_oid from pg_class where relnamespace::regnamespace::name=v_hist_schema and relname=v_hist_table_name;
  if v_hist_table_oid is null then
    with att as (
      select attname,attnum,typname 
	    from pg_attribute a
	    join pg_type t on t.oid=a.atttypid 
        where attrelid=v_table_oid and attnum>0 and not attisdropped
	) 
    select array_agg(attname||' '||typname order by attnum),
           array_agg('comment on column '||v_hist_schema||'.'||v_hist_table_name||'.'||attname||' is '||quote_literal(desc_expr) order by attnum),
		   array_agg(attname order by attnum),
           array_agg(coalesce('('||desc_expr||')','row.'||attname) order by attnum)           
           into v_hist_columns_ddl,v_hist_columns_ddl_desc,v_hist_columns_name,v_hist_row_values
      from (
        select attname,typname,attnum,null desc_expr from att
        union all
	    select * from (
          select pghist.hist_column_desc_name(attname),'text',attnum+0.1,pghist.hist_column_desc_expression_default(v_schema,v_table_name,attname) desc_expr from att
	    ) columns_expr where desc_expr is not null     
	  ) columns;	  
    v_sql := 'create table '||v_hist_schema||'.'||v_hist_table_name||' ('||v_newline;  
    v_sql := v_sql||'  hist_id bigint,'||v_newline;
    v_sql := v_sql||'  hist_transaction_id bigint not null references pghist.hist_transaction(id) on delete cascade,'||v_newline;
    v_sql := v_sql||'  hist_operation varchar(11) not null check (hist_operation in (''HIST_CREATE'',''INSERT'',''UPDATE'',''DELETE'',''TRUNCATE'')),'||v_newline;
    v_sql := v_sql||'  hist_state varchar(3) not null check (hist_state in (''OLD'',''NEW'')),'||v_newline;
    v_sql := v_sql||'  primary key (hist_id,hist_state),'||v_newline;
    v_sql := v_sql||'  hist_pkey varchar[],'||v_newline;  
    v_sql := v_sql||'  hist_row_desc text,'||v_newline;   
    v_sql := v_sql||'  '||array_to_string(v_hist_columns_ddl,',')||v_newline;
    v_sql := v_sql||')';
    call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
    call pghist.hist_execute_sql(v_schema, v_table_name, 'create index on '||v_hist_schema||'.'||v_hist_table_name||'(hist_transaction_id)');
    v_table_row_desc_expr := pghist.hist_row_desc_expression_default(v_schema, v_table_name);
	if v_table_row_desc_expr is not null then   
      call pghist.hist_execute_sql(schema, table_name, 'comment on table '||v_hist_schema||'.'||v_hist_table_name||' is '||quote_literal(v_table_row_desc_expr));
	else
	  v_table_row_desc_expr := 'null';
	end if;
	for rec in (select * from unnest(v_hist_columns_ddl_desc) ddl_desc where ddl_desc is not null) loop
   	  call pghist.hist_execute_sql(v_schema, v_table_name, rec.ddl_desc);
  	end loop;    
    call pghist.hist_execute_sql(v_schema, v_table_name, 'lock table '||v_hist_schema||'.'||v_hist_table_name||' in exclusive mode');
    v_sql := 'insert into '||v_hist_schema||'.'||v_hist_table_name||' ('||v_hist_table_fix_columns||array_to_string(v_hist_columns_name,',')||')'; 
    v_sql := v_sql||'  select nextval(''pghist.hist_id_seq''),'||pghist.hist_transaction_id()||',''HIST_CREATE'',''NEW'','||v_hist_row_values_pkey||',('||v_table_row_desc_expr||'),'||array_to_string(v_hist_row_values,',')||' from only '||v_schema||'.'||v_table_name||' row';
    call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);   
    call pghist.hist_execute_sql(v_schema, v_table_name, 'grant select on '||v_hist_schema||'.hist_transaction,'||v_hist_schema||'.'||v_hist_table_name||' to '||v_table_owner);
    v_hist_table_oid := (v_hist_schema||'.'||v_hist_table_name)::regclass::oid;
  else
    for rec in
      with cols as (select attname,atttypid from pg_attribute where attrelid=v_table_oid and attnum>0 and not attisdropped)
	  select 'alter table '||v_hist_schema||'.'||v_hist_table_name||' drop column '||attname sql_drop_column
	    from pg_attribute a
	    where attrelid=v_hist_table_oid and attnum>v_hist_table_fix_column_count and not attisdropped
          and (attname,atttypid) not in (select * from cols)
          and attname not in (select pghist.hist_column_desc_name(attname) from cols) 
    loop
      call pghist.hist_execute_sql(v_schema, v_table_name, 'drop view if exists '||v_view_hist_name);
      call pghist.hist_execute_sql(v_schema, v_table_name, rec.sql_drop_column);
    end loop;
    for rec in   
      select attname column_name, typname column_type, pg_get_expr(d.adbin,d.adrelid) default_value, pghist.hist_column_desc_expression_default(v_schema,v_table_name,attname) desc_expr, pghist.hist_column_desc_name(attname) column_desc_name
	    from pg_attribute a
	    join pg_type t on t.oid=a.atttypid
	    left join pg_attrdef d on d.adrelid=a.attrelid and d.adnum=a.attnum 
	    where attrelid=v_table_oid and attnum>0 and not attisdropped
          and attname not in (select attname from pg_attribute where attrelid=v_hist_table_oid and attnum>0 and not attisdropped)                   
        order by attnum          
    loop
      call pghist.hist_execute_sql(v_schema, v_table_name, 'alter table '||v_hist_schema||'.'||v_hist_table_name||' add column '||rec.column_name||' '||rec.column_type);
      if rec.default_value is not null then
        call pghist.hist_execute_sql(v_schema, v_table_name, 'lock table '||v_hist_schema||'.'||v_hist_table_name||' in exclusive mode');
        call pghist.hist_execute_sql(v_schema, v_table_name, 'update '||v_hist_schema||'.'||v_hist_table_name||' set '||rec.column_name||'='||rec.default_value);      
      end if; 
      if rec.desc_expr is not null then
        call pghist.hist_execute_sql(v_schema, v_table_name, 'alter table '||v_hist_schema||'.'||v_hist_table_name||' add column '||rec.column_desc_name||' text');
        call pghist.hist_execute_sql(v_schema, v_table_name, 'comment on column '||v_hist_schema||'.'||v_hist_table_name||'.'||rec.column_desc_name||' is '||quote_literal(rec.desc_expr));       
      end if;
    end loop;
    select coalesce(max(description),'null') into v_table_row_desc_expr from pg_description where objoid=v_hist_table_oid and objsubid=0;
    select array_agg(attname order by attnum),
           array_agg(case when attname like '%$hist_desc' then '('||col_description(v_hist_table_oid,attnum)||')' else 'row.'||attname end order by attnum)           
           into v_hist_columns_name,v_hist_row_values
      from pg_attribute a
      where attrelid=v_hist_table_oid and attnum>v_hist_table_fix_column_count and not attisdropped;
  end if;
  select array_agg((select attname from pg_attribute where attrelid=v_hist_table_oid and attname=column_name||'$hist_desc'))
         into v_columns_hist_desc
    from unnest(v_columns_name) column_name;
  for rec in
    select 'drop index '||v_hist_schema||'.'||c.relname sql_drop_index 
      from pg_index i
      join pg_class c on c.oid=i.indexrelid
      where indrelid = v_hist_table_oid      
        and indkey not in (select indkey from pg_index where indrelid=v_table_oid)
        and (select min(unnest) from (select unnest(indkey)) indkey)>v_hist_table_fix_column_count
        and (select array_agg(attname order by array_position(i.indkey, a.attnum)) from pg_attribute a where a.attrelid=i.indrelid and a.attnum = any(i.indkey)) not in  
              (select (
                 select array_agg(attname order by array_position(i.indkey, a.attnum)) from pg_attribute a  where a.attrelid=i.indrelid and a.attnum = any(i.indkey)
              ) from pg_index i where indrelid = v_table_oid)
  loop
    call pghist.hist_execute_sql(schema, table_name, rec.sql_drop_index);
  end loop;
  for rec in 
    select 'create index on '||v_hist_schema||'.'||v_hist_table_name||'('||
           (select string_agg(attname,',' order by array_position(i.indkey, a.attnum)) from pg_attribute a where a.attrelid=i.indrelid and a.attnum = any(i.indkey))||
           ')' sql_create_index 
      from pg_index i
      where indrelid=v_table_oid
        and (select array_agg(attname order by array_position(i.indkey, a.attnum)) from pg_attribute a where a.attrelid=i.indrelid and a.attnum = any(i.indkey)) not in  
              (select (
                 select array_agg(attname order by array_position(i.indkey, a.attnum)) from pg_attribute a  where a.attrelid=i.indrelid and a.attnum = any(i.indkey)
              ) from pg_index i where indrelid = v_hist_table_oid)
  loop
    call pghist.hist_execute_sql(v_schema, v_table_name, rec.sql_create_index);
  end loop;
  --
  v_sql := 'create or replace function '||v_trigger_iud_function_name||'() returns trigger language plpgsql security definer as $$'||v_newline;
  v_sql := v_sql||'declare '||v_newline;
  v_sql := v_sql||'    v_transaction_id bigint;'||v_newline;
  v_sql := v_sql||'    v_hist_id bigint;'||v_newline;
  v_sql := v_sql||'begin '||v_newline;
  v_sql := v_sql||'    v_transaction_id := '||v_hist_schema||'.hist_transaction_id();'||v_newline;
  v_sql := v_sql||'    v_hist_id := nextval(''pghist.hist_id_seq'');'||v_newline;
  v_sql := v_sql||'    if (tg_op in (''INSERT'',''UPDATE'')) then'||v_newline;
  v_sql := v_sql||'        insert into '||v_hist_schema||'.'||v_hist_table_name||' ('||v_hist_table_fix_columns||array_to_string(v_hist_columns_name,',')||') values ('||v_hist_table_fix_values||'''NEW'','||regexp_replace(v_hist_row_values_pkey||',('||v_table_row_desc_expr||'),'||array_to_string(v_hist_row_values,','),'row\.','new.','gi')||');'||v_newline;
  v_sql := v_sql||'    end if;'||v_newline;
  v_sql := v_sql||'    if (tg_op in (''UPDATE'',''DELETE'')) then'||v_newline;
  v_sql := v_sql||'        insert into '||v_hist_schema||'.'||v_hist_table_name||' ('||v_hist_table_fix_columns||array_to_string(v_hist_columns_name,',')||') values ('||v_hist_table_fix_values||'''OLD'','||regexp_replace(v_hist_row_values_pkey||',('||v_table_row_desc_expr||'),'||array_to_string(v_hist_row_values,','),'row\.','old.','gi')||');'||v_newline;
  v_sql := v_sql||'    end if;'||v_newline;
  v_sql := v_sql||'    return null;'||v_newline;
  v_sql := v_sql||'end;'||v_newline;
  v_sql := v_sql||'$$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql); 
  if not exists (select 1 from pg_trigger where tgrelid=v_table_oid and tgname=v_trigger_iud_name) then
    call pghist.hist_execute_sql(v_schema, v_table_name, 'create trigger '||v_trigger_iud_name||' after insert or update or delete on '||schema||'.'||table_name||' for each row execute procedure '||v_trigger_iud_function_name||'();');
  end if;
  v_sql := 'create or replace function '||v_trigger_truncate_function_name||'() returns trigger language plpgsql security definer as $$'||v_newline;
  v_sql := v_sql||'declare '||v_newline;
  v_sql := v_sql||'  v_transaction_id bigint;'||v_newline;
  v_sql := v_sql||'begin '||v_newline;
  v_sql := v_sql||'  v_transaction_id := pghist.hist_transaction_id();'||v_newline;
  v_sql := v_sql||'  insert into '||v_hist_schema||'.'||v_hist_table_name||' ('||v_hist_table_fix_columns||array_to_string(v_hist_columns_name,',')||')'||v_newline; 
  v_sql := v_sql||'    select nextval(''pghist.hist_id_seq''),v_transaction_id,''TRUNCATE'',''OLD'','||v_hist_row_values_pkey||',('||v_table_row_desc_expr||'),'||array_to_string(v_hist_row_values,',')||' from only '||v_schema||'.'||v_table_name||' row;'||v_newline;
  v_sql := v_sql||'  return null;'||v_newline;
  v_sql := v_sql||'end;'||v_newline;
  v_sql := v_sql||'$$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  if not exists (select 1 from pg_trigger where tgrelid=v_table_oid and tgname=v_trigger_truncate_name) then
    call pghist.hist_execute_sql(v_schema, v_table_name, 'create trigger '||v_trigger_truncate_name||' before truncate on '||schema||'.'||table_name||' execute procedure '||v_trigger_truncate_function_name||'();');
  end if;
  --  
  v_sql := 'create or replace view '||v_view_hist_name||' as '||v_newline;
  v_sql := v_sql||'select t.xact_start hist_timestamp,h.hist_operation,t.db_user hist_db_user,t.app_user hist_app_user,t.application_name hist_application_name,t.id hist_transaction_id,h.hist_id,'||pghist.hist_columns_to_text(v_columns_pkey, '''h.''||col')||','||pghist.hist_columns_to_text(v_columns_name, 'col||''_old,''||col||''_new''')||v_newline;
  v_sql := v_sql||'  from ('||v_newline; 
  v_sql := v_sql||'    select hist_id,hist_transaction_id,hist_operation,'||pghist.hist_columns_to_text(v_columns_pkey, 'col')||','||pghist.hist_columns_to_text(v_columns_name, '''null ''||col||''_old,''||col||'' ''||col||''_new''')||' from pghist.'||v_hist_table_name||' where hist_operation in (''HIST_CREATE'',''INSERT'')'||v_newline;
  v_sql := v_sql||'    union all'||v_newline;
  v_sql := v_sql||'    select un.hist_id,un.hist_transaction_id,un.hist_operation,'||pghist.hist_columns_to_text(v_columns_pkey, '''un.''||col')||','||pghist.hist_columns_to_text(v_columns_name, '''uo.''||col||'',un.''||col')||' from pghist.'||v_hist_table_name||' un '||v_newline;    
  v_sql := v_sql||'      join pghist.'||v_hist_table_name||' uo on uo.hist_id=un.hist_id and uo.hist_state=''OLD'''||v_newline;
  v_sql := v_sql||'      where un.hist_operation= ''UPDATE'' and un.hist_state=''NEW'''||v_newline;
  v_sql := v_sql||'    union all'||v_newline;
  v_sql := v_sql||'    select hist_id,hist_transaction_id,hist_operation,'||pghist.hist_columns_to_text(v_columns_pkey, 'col')||','||pghist.hist_columns_to_text(v_columns_name, 'col||'',null''')||' from pghist.'||v_hist_table_name||' where hist_operation in (''DELETE'',''TRUNCATE'')'||v_newline;
  v_sql := v_sql||'  ) h'||v_newline;  
  v_sql := v_sql||'  join pghist.hist_transaction t on t.id=h.hist_transaction_id'||v_newline;  
  v_sql := v_sql||'  order by hist_id';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'alter view '||v_view_hist_name||' owner to '||v_table_owner);
  --
  v_sql := 'create or replace function '||v_table_changes_name||'(where_clause text default null, where_param anyelement default null::varchar, cascade boolean default true) returns setof pghist.table_change language plpgsql security definer as $$'||v_newline;
  v_sql := v_sql||'declare'||v_newline;
  v_sql := v_sql||'  column_comments varchar[] := array['||array_to_string(v_columns_comment,',')||'];'||v_newline;
  v_sql := v_sql||'  rec_change pghist.table_change;'||v_newline;
  v_sql := v_sql||'  rec_hist record;'||v_newline;
  v_sql := v_sql||'  rec_hist_update_old record;'||v_newline;
  v_sql := v_sql||'  cur_hist refcursor;'||v_newline;
  v_sql := v_sql||'  v_query text;'||v_newline; 
  v_sql := v_sql||'begin'||v_newline;
  v_sql := v_sql||'  v_query := ''select hist_row_desc,t.db_user hist_db_user,t.app_user hist_app_user,t.xact_start hist_timestamp,h.*'';'||v_newline;
  v_sql := v_sql||'  v_query := v_query||''  from '||v_hist_schema||'.'||v_hist_table_name||' h'';'||v_newline;
  v_sql := v_sql||'  v_query := v_query||''  join '||v_hist_schema||'.hist_transaction t on t.id=h.hist_transaction_id'';'||v_newline; 
  v_sql := v_sql||'  v_query := v_query||coalesce(''  where hist_id in (select hist_id from '||v_hist_schema||'.'||v_hist_table_name||' row where ''||where_clause||'')'','''');'||v_newline;
  v_sql := v_sql||'  v_query := v_query||''  order by hist_id,hist_state'';'||v_newline; 
  v_sql := v_sql||'  open cur_hist for execute v_query using where_param;'||v_newline;
  v_sql := v_sql||'  rec_change.schema := '''||v_schema||''';'||v_newline;
  v_sql := v_sql||'  rec_change.table_name := '''||v_table_name||''';'||v_newline;
  v_sql := v_sql||'  rec_change.table_comment := '||quote_nullable(obj_description(v_table_oid))||';'||v_newline; 
  v_sql := v_sql||'  loop'||v_newline;
  v_sql := v_sql||'    fetch cur_hist into rec_hist;'||v_newline;
  v_sql := v_sql||'    exit when not found;'||v_newline;
  v_sql := v_sql||'    rec_change.row_desc := rec_hist.hist_row_desc;'||v_newline; 
  v_sql := v_sql||'    rec_change.db_user := rec_hist.hist_db_user;'||v_newline; 
  v_sql := v_sql||'    rec_change.app_user := rec_hist.hist_app_user;'||v_newline;
  v_sql := v_sql||'    rec_change.timestamp := rec_hist.hist_timestamp;'||v_newline;
  v_sql := v_sql||'    rec_change.operation := rec_hist.hist_operation;'||v_newline;
  v_sql := v_sql||'    rec_change.row_pkey := rec_hist.hist_pkey;'||v_newline;
  v_sql := v_sql||'    rec_change.transaction_id  := rec_hist.hist_transaction_id ;'||v_newline; 
  v_sql := v_sql||'    rec_change.hist_id  := rec_hist.hist_id ;'||v_newline;
  v_sql := v_sql||'    if rec_hist.hist_operation = ''INSERT'' then'||v_newline;
  v_sql := v_sql||'      rec_change.value_old := null;'||v_newline; 
  v_sql := v_sql||'      rec_change.value_old_desc := null;'||v_newline; 
  for i in 1..array_length(v_columns_name, 1) loop
    v_sql := v_sql||'      if rec_hist.'||v_columns_name[i]||' is not null then rec_change.column_name := '''||v_columns_name[i]||'''; rec_change.column_pos := '||v_columns_pos[i]||'; rec_change.column_comment := column_comments['||i||']; rec_change.value_new := rec_hist.'||v_columns_name[i]||'; rec_change.value_new_desc := '||coalesce('rec_hist.'||v_columns_hist_desc[i],'null')||'; return next rec_change; end if;'||v_newline;
  end loop;
  v_sql := v_sql||'    end if;'||v_newline;
  v_sql := v_sql||'    if rec_hist.hist_operation = ''UPDATE'' then'||v_newline;
  v_sql := v_sql||'      fetch cur_hist into rec_hist_update_old;'||v_newline;
  v_sql := v_sql||'      exit when not found;'||v_newline;
  for i in 1..array_length(v_columns_name, 1) loop
    v_type_convert := case when v_columns_type[i]='json' then '::text' else '' end;
    v_sql := v_sql||'      if (rec_hist.'||v_columns_name[i]||v_type_convert||' != rec_hist_update_old.'||v_columns_name[i]||v_type_convert||') or (rec_hist.'||v_columns_name[i]||' is not null and rec_hist_update_old.'||v_columns_name[i]||' is null) or (rec_hist.'||v_columns_name[i]||' is null and rec_hist_update_old.'||v_columns_name[i]||' is not null) then rec_change.column_name := '''||v_columns_name[i]||'''; rec_change.column_pos := '||v_columns_pos[i]||'; rec_change.column_comment := column_comments['||i||']; rec_change.value_old := rec_hist_update_old.'||v_columns_name[i]||'; rec_change.value_old_desc := '||coalesce('rec_hist_update_old.'||v_columns_hist_desc[i],'null')||'; rec_change.value_new := rec_hist.'||v_columns_name[i]||'; rec_change.value_new_desc := '||coalesce('rec_hist.'||v_columns_hist_desc[i],'null')||'; return next rec_change; end if;'||v_newline;
  end loop;
  v_sql := v_sql||'    end if;'||v_newline;
  v_sql := v_sql||'    if rec_hist.hist_operation in (''DELETE'',''TRUNCATE'') then'||v_newline;
  v_sql := v_sql||'      rec_change.value_new := null;'||v_newline;
  v_sql := v_sql||'      rec_change.value_new_desc := null;'||v_newline; 
  for i in 1..array_length(v_columns_name, 1) loop
    v_sql := v_sql||'      if rec_hist.'||v_columns_name[i]||' is not null then rec_change.column_name := '''||v_columns_name[i]||'''; rec_change.column_pos := '||v_columns_pos[i]||'; rec_change.column_comment := column_comments['||i||']; rec_change.value_old := rec_hist.'||v_columns_name[i]||'; rec_change.value_old_desc := '||coalesce('rec_hist.'||v_columns_hist_desc[i],'null')||'; return next rec_change; end if;'||v_newline;
  end loop;
  v_sql := v_sql||'    end if;'||v_newline;
  v_sql := v_sql||'  end loop;'||v_newline;
  v_sql := v_sql||'  close cur_hist;'||v_newline;
  if v_children is not null then    
    v_sql := v_sql||'  if cascade then'||v_newline;
    for i in 1..array_length(v_children, 1) loop
      v_sql := v_sql||'    for rec_change in select * from '||v_children[i][1]||'.'||v_children[i][2]||'_changes(where_clause, where_param, true) where column_name in ('''||array_to_string(v_columns_name,''',''')||''') loop'||v_newline;     
      v_sql := v_sql||'      return next rec_change;'||v_newline;
       v_sql := v_sql||'    end loop;'||v_newline;
    end loop;   
    v_sql := v_sql||'  end if;'||v_newline;
  end if; 
  v_sql := v_sql||'end;'||v_newline;
  v_sql := v_sql||'$$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);  
  call pghist.hist_execute_sql(v_schema, v_table_name, 'alter function '||v_table_changes_name||' owner to '||v_table_owner);
  --
  if array_length(v_columns_pkey,1) is not null then  
    v_sql := 'create or replace function '||v_table_at_timestamp_name||'(transaction_start timestamptz, cascade boolean default true) returns setof '||v_schema||'.'||v_table_name||' language plpgsql security definer as $$'||v_newline;
    v_sql := v_sql||'declare'||v_newline;
    v_sql := v_sql||'  rec '||v_schema||'.'||v_table_name||'%rowtype;'||v_newline;
    v_sql := v_sql||'begin'||v_newline;
    v_sql := v_sql||'  for rec in '||v_newline;
    v_sql := v_sql||'    select '||array_to_string(v_columns_name,',')||' from ('||v_newline;
    v_sql := v_sql||'      select row_number() over (partition by '||pghist.hist_columns_to_text(v_columns_pkey, '''h.''||col')||' order by hist_id desc,hist_state) hist_row_number,h.*'||v_newline;
    v_sql := v_sql||'         from '||v_hist_schema||'.'||v_hist_table_name||' h'||v_newline;
    v_sql := v_sql||'         join pghist.hist_transaction t on t.id=h.hist_transaction_id and t.xact_start<transaction_start'||v_newline;      
    v_sql := v_sql||'    ) h where hist_row_number=1 and hist_state=''NEW'''||v_newline;
    v_sql := v_sql||'    loop'||v_newline;   
    v_sql := v_sql||'      return next rec;'||v_newline;
    v_sql := v_sql||'  end loop;'||v_newline;
    if v_children is not null then    
      v_sql := v_sql||'  if cascade then'||v_newline;
      for i in 1..array_length(v_children, 1) loop
        v_sql := v_sql||'    for rec in select '||array_to_string(v_columns_name,',')||' from '||v_children[i][1]||'.'||v_children[i][2]||'_at_timestamp(transaction_start, true) loop'||v_newline;
        v_sql := v_sql||'      return next rec;'||v_newline;
        v_sql := v_sql||'    end loop;'||v_newline;
      end loop;   
      v_sql := v_sql||'  end if;'||v_newline;
    end if; 
    v_sql := v_sql||'  return;'||v_newline;   
    v_sql := v_sql||'end;'||v_newline;   
    v_sql := v_sql||'$$';  
    call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
    call pghist.hist_execute_sql(v_schema, v_table_name, 'alter function '||v_table_at_timestamp_name||' owner to '||v_table_owner);   
  else
    call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_table_at_timestamp_name||'(timestamptz, boolean)');
  end if;
end; $body$;

create or replace procedure pghist.hist_enable(table_name name) language plpgsql as $$
begin
  call pghist.hist_enable(current_schema(), table_name);
end; $$;

create or replace procedure pghist.hist_row_desc_expression(schema name, table_name name, expression varchar) language plpgsql as $$
begin
  call pghist.hist_execute_sql(schema, table_name, 'comment on table pghist.'||pghist.hist_table_name(schema,table_name)||' is '||coalesce(quote_literal(expression),'null'));
  call pghist.hist_enable(schema, table_name); 
end; $$;

create or replace function pghist.hist_row_desc_expression_default(schema name, table_name name) returns varchar language plpgsql as $$
declare
  v_table_oid oid := (schema||'.'||table_name)::regclass::oid;
begin
  if not exists (select 1 from pg_index where indrelid = v_table_oid and indisprimary) then 
    return null;
  end if;
  return (
    select quote_literal(coalesce(description, table_name)||' #')||'||'||pk.expr from (
      select string_agg('row.'||attname, '||'',''||' order by col_pos) expr
        from unnest( (select conkey from pg_constraint where conrelid=v_table_oid and contype='p') ) with ordinality c(col_num,col_pos)
        join pg_attribute a on attrelid=v_table_oid and attnum = col_num
      ) pk
      left join pg_description d on d.objoid=v_table_oid and d.objsubid=0
  );
end; $$;

create or replace function pghist.hist_row_desc_expression_current(schema name, table_name name) returns varchar language plpgsql as $$
begin
  return col_description(('pghist.'||pghist.hist_table_name(schema,table_name))::regclass::oid,0);	
end; $$;

create or replace procedure pghist.hist_column_desc_expression(schema name, table_name name, column_name name, expression varchar) language plpgsql as $$
declare
  v_hist_table name := 'pghist.'||pghist.hist_table_name(schema,table_name);
  v_hist_column_desc name := pghist.hist_column_desc_name(column_name);
begin
  call pghist.hist_execute_sql(schema, table_name, 'alter table '||v_hist_table||' add column if not exists '||v_hist_column_desc||' text');	
  call pghist.hist_execute_sql(schema, table_name, 'comment on column '||v_hist_table||'.'||v_hist_column_desc||' is '||coalesce(quote_literal(expression),'null'));
  call pghist.hist_enable(schema, table_name); 
end; $$;

create or replace function pghist.hist_column_desc_expression_default(schema name, table_name name, column_name name) returns varchar language plpgsql as $$
declare
  v_table_oid oid := (schema||'.'||table_name)::regclass::oid;
  v_column_name name := lower(column_name);
begin
  return (
    select 'select '||f_text.attname||' from '||f_tab.relnamespace::regnamespace::name||'.'||f_tab.relname||' where '||f_key.attname||'=row.'||v_column_name  
      from pg_constraint c
      join pg_attribute ca on  ca.attrelid=c.conrelid and ca.attnum=c.conkey[1] and ca.attname=v_column_name 
      join pg_class f_tab on f_tab.oid=c.confrelid
      join pg_attribute f_key on f_key.attrelid=c.confrelid and f_key.attnum=c.confkey[1]
      join pg_attribute f_text on f_text.attrelid=c.confrelid and f_text.attnum!=f_key.attnum 
      join pg_type t on t.oid=f_text.atttypid and t.typcategory='S'
      where c.conrelid=v_table_oid and c.contype='f' and array_length(c.conkey,1)=1
      order by f_text.attnum
      limit 1
  );
end; $$;

create or replace function pghist.hist_column_desc_expression_current(schema name, table_name name, column_name name) returns varchar language plpgsql as $$
begin
  return ( select col_description(attrelid,attnum) from pg_attribute where attrelid=('pghist.'||pghist.hist_table_name(schema,table_name))::regclass::oid and attname=pghist.hist_column_desc_name(column_name) );	
end; $$;

create or replace procedure pghist.hist_disable(schema name, table_name name) language plpgsql as $$
declare
  v_schema name := lower(schema);
  v_table_name name := lower(table_name);
  v_hist_schema name;  
  v_hist_table_name name;
  v_trigger_iud_function_name name;
  v_trigger_iud_name name;
  v_trigger_truncate_function_name name;
  v_trigger_truncate_name name;
  v_view_hist_name name;
  v_table_changes_name name;
  v_table_at_timestamp_name name;
begin
  call pghist.hist_object_names(v_schema, v_table_name, v_hist_schema, v_hist_table_name, v_trigger_iud_function_name, v_trigger_iud_name, v_trigger_truncate_function_name, v_trigger_truncate_name, v_view_hist_name, v_table_changes_name, v_table_at_timestamp_name);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop view if exists '||v_view_hist_name); 
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_table_changes_name||'(boolean, text, text, anyelement, anyelement, anyelement)');
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_table_at_timestamp_name||'(timestamptz, boolean)');
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_trigger_iud_function_name||' cascade');
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_trigger_truncate_function_name||' cascade');
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop table if exists '||v_hist_schema||'.'||v_hist_table_name||' cascade');  
end; $$;

create or replace procedure pghist.hist_disable(table_name name) language plpgsql as $$
begin
  call pghist.hist_disable(current_schema(), table_name);
end; $$;

create or replace function pghist.event_fn_ddl_command() returns event_trigger security definer as $$
declare
  rec record;
begin
  for rec in select * from ( 
    select schema_name,relname table_name
      from pg_event_trigger_ddl_commands() e
      join pg_class c on c.oid=e.objid
      where object_type in ('table','table column')
    ) t where pghist.hist_exists(schema_name,table_name) 
  loop
    call pghist.hist_enable(rec.schema_name, rec.table_name);
  end loop;
  for rec in select * from ( 
    select schema_name schema_name,relname table_name
      from pg_event_trigger_ddl_commands() e
      join pg_index i on i.indexrelid=e.objid
      join pg_class c on c.oid=i.indrelid
      where object_type in ('index')
    ) t where pghist.hist_exists(schema_name,table_name) 
  loop
    call pghist.hist_enable(rec.schema_name, rec.table_name);
  end loop;	
end;
$$ language plpgsql;

create or replace function pghist.event_fn_drop_table() returns event_trigger security definer as $$
declare
  rec record;
begin  	
  for rec in 
    select schema_name,object_name table_name 
     from pg_event_trigger_dropped_objects()
     where object_type='table' and pghist.hist_exists(schema_name,object_name)
  loop
    call pghist.hist_disable(rec.schema_name, rec.table_name);
  end loop;	
end;
$$ language plpgsql;

do $$ begin
  if not exists (select 1 from pg_event_trigger where evtname='pghist_event_tg_ddl_command') then
    create event trigger pghist_event_tg_ddl_command on ddl_command_end when tag in ('ALTER TABLE','CREATE INDEX','COMMENT') execute procedure pghist.event_fn_ddl_command();
  end if;
  if not exists (select 1 from pg_event_trigger where evtname='pghist_event_tg_drop_table') then
    create event trigger pghist_event_tg_drop_table on sql_drop when tag in ('DROP TABLE','DROP SCHEMA') execute function pghist.event_fn_drop_table();
  end if; 
end $$;create schema if not exists pghist;

create or replace function pghist.pghist_version() returns varchar language plpgsql as $$
begin
  return '24.2.5'; -- 2024.04.09 18:19:19
end; $$;

create table if not exists pghist.hist_transaction(
  id bigint primary key,
  xid bigint not null,  
  timestamp_start timestamptz not null,
  timestamp_commit timestamptz,
  application_name varchar,
  backend_pid integer not null,
  backend_start timestamptz not null,  
  db_user name not null,
  db_client_addr inet,
  db_client_hostname varchar,
  app_user name,
  app_client_addr inet,
  app_client_hostname varchar  
);
create sequence if not exists pghist.hist_transaction_seq as bigint increment 2;

create table if not exists pghist.hist_query(
  hash bigint primary key,
  text text not null  
);
  
create table if not exists pghist.hist_statement(
  id bigint primary key,
  transaction_id bigint not null, -- references pghist.hist_transaction(id) on delete cascade,
  timestamp timestamptz not null,
  operation varchar(16) not null check (operation in ('HIST_ENABLE','INSERT','UPDATE','DELETE','TRUNCATE')),
  query_hash bigint not null -- references pghist.hist_query(hash)   
);
create sequence if not exists pghist.hist_statement_seq as bigint increment 2;

create table if not exists pghist.hist_sql_log(
  id serial primary key,
  pghist_version varchar(8) not null default pghist.pghist_version(),
  transaction_id bigint not null references pghist.hist_transaction(id) on delete cascade,  
  schema name not null,
  table_name name not null,  
  sql_statement text not null
);

do $$ begin
  if to_regtype('pghist.table_change') is null then
    create type pghist.table_change as (
      statement_num bigint,
      row_num int,
      column_num int,
      timestamp timestamptz,
      operation varchar,
      operation_name varchar,
      column_name name,
      column_comment varchar,      
      value_old text,
      value_old_desc text,      
      value_new text,
      value_new_desc text,
      row_desc text,      
      db_user varchar,
      db_user_name varchar,
      app_user varchar,
      app_user_name varchar,
      schema name,
      table_name name,
      table_comment text     
    );  
  end if;
end $$;

create or replace function pghist.hist_transaction_id() returns bigint language plpgsql as $$
declare
  v_id_text varchar;
  v_id bigint;
begin
  v_id_text := current_setting('pghist.transaction_id', true);
  if v_id_text!='' then
    return v_id_text::bigint;
  end if; 
  v_id := reverse(nextval('pghist.hist_transaction_seq')::varchar);
  insert into pghist.hist_transaction(id, xid, timestamp_start, application_name, backend_pid, backend_start, db_user, db_client_addr, db_client_hostname, app_user, app_client_addr, app_client_hostname)
    select v_id, txid_current(), xact_start, application_name, pg_backend_pid(), backend_start, usename, client_addr, client_hostname, pghist.hist_custom_app_user(), pghist.hist_custom_app_client_addr(), pghist.hist_custom_app_client_hostname()
    from pg_stat_activity p where pid = pg_backend_pid();
  perform set_config('pghist.transaction_id', v_id::varchar, true);
  return v_id;
end; $$;

create or replace function pghist.hist_transaction_fn_commit() returns trigger language plpgsql as $$
begin
  update pghist.hist_transaction set timestamp_commit = clock_timestamp() where id=new.id;  
  return null;
end; $$;

do $$ begin
  if not exists (select from pg_trigger where tgrelid='pghist.hist_transaction'::regclass::oid and tgname='hist_transaction_tg_commit') then
    create constraint trigger hist_transaction_tg_commit after insert on pghist.hist_transaction deferrable initially deferred for each row execute procedure pghist.hist_transaction_fn_commit();  
  end if; 
end $$;

create or replace function pghist.hist_statement_id(operation varchar) returns bigint language plpgsql as $$
declare
  v_id bigint := reverse(nextval('pghist.hist_statement_seq')::varchar);
  v_transaction_id bigint = pghist.hist_transaction_id();
  v_query_text text = current_query();
  v_query_hash bigint := hashtextextended(v_query_text, 0);  
begin
  insert into pghist.hist_query(hash, text) values (v_query_hash, v_query_text) on conflict (hash) do nothing;
  insert into pghist.hist_statement(id, transaction_id, timestamp, operation, query_hash)
    values (v_id, v_transaction_id, clock_timestamp(), operation, v_query_hash);
  return v_id;
end; $$;

create or replace procedure pghist.hist_execute_sql(schema name, table_name name, sql_statement varchar) language plpgsql as $$
begin
  execute sql_statement;      
  insert into pghist.hist_sql_log(transaction_id,schema,table_name,sql_statement)
    values(pghist.hist_transaction_id(),schema,table_name,sql_statement);  
end; $$;

create or replace function pghist.hist_table_name(schema name, table_name name) returns varchar language plpgsql as $$
begin
  return 'hist_'||schema||'_'||table_name;
end; $$;

create or replace function pghist.hist_column_name(schema name, table_name name, column_name name) returns varchar language plpgsql as $$
begin
  return column_name||case when column_name=any(col_description(('pghist.'||pghist.hist_table_name(schema,table_name))::regclass::oid,3)::name[]) then '' else '_old' end;	
end; $$;

create or replace function pghist.hist_exists(schema name, table_name name) returns boolean language plpgsql as $$
begin
  return exists (select 1 from pg_tables where schemaname='pghist' and tablename=pghist.hist_table_name(schema,table_name));
end; $$;

create or replace function pghist.hist_columns_to_text(columns name[], expr text default 'col', delimiter varchar default ',') returns varchar language plpgsql as $$
declare
  v_text text;
begin
  if pg_catalog.array_length(columns, 1) is null then return ''; end if;	
  execute format('select string_agg(%s, ''%s'' order by pos) from unnest($1) with ordinality r(col,pos)', expr, delimiter )
    into v_text
    using columns;	
  return v_text;  
end; $$;

create or replace procedure pghist.hist_object_names(schema name, table_name name, inout hist_schema name, inout hist_table_name name, inout trigger_iud_function_name name, inout trigger_iud_prefix name, inout trigger_truncate_function_name name, inout trigger_truncate_name name, inout view_hist_name name, inout table_changes_name name, inout table_at_timestamp_name name) language plpgsql as $$
begin
  hist_schema := 'pghist';	
  hist_table_name := pghist.hist_table_name(schema,table_name);
  --
  trigger_iud_function_name      := 'hist_'||table_name||'_tg_fn_iud';
  trigger_iud_prefix             := 'hist_'||table_name||'_tg_';
  trigger_truncate_function_name := 'hist_'||table_name||'_tg_fn_truncate';
  trigger_truncate_name          := 'hist_'||table_name||'_tg_truncate';
  --
  view_hist_name          := table_name||'_hist';  
  table_changes_name      := table_name||'_changes';
  table_at_timestamp_name := table_name||'_at_timestamp';
end; $$;

create or replace procedure pghist.hist_enable(table_name name) security definer language plpgsql as $$
begin
  call pghist.hist_enable(current_schema(), table_name);
end; $$;

create or replace procedure pghist.hist_enable_parent(schema name, table_name name) language plpgsql as $$
declare
  v_parent_schema name;
  v_parent_table_name name;
begin
  select c.relnamespace::regnamespace::name,c.relname
    into v_parent_schema,v_parent_table_name
      from pg_inherits i
    join pg_class c on c.oid=i.inhparent
    where i.inhrelid = to_regclass(schema||'.'||table_name)::oid and pghist.hist_exists(c.relnamespace::regnamespace::name,c.relname);
  if v_parent_schema is not null then
    call pghist.hist_enable(v_parent_schema,v_parent_table_name);    
  end if;
end; $$;

create or replace procedure pghist.hist_disable(schema name, table_name name) security definer language plpgsql as $$
declare
  v_schema name := lower(schema);
  v_table_name name := lower(table_name);
  v_table_oid oid := to_regclass(v_schema||'.'||v_table_name)::oid;
  v_hist_schema name;  
  v_hist_table_name name;
  v_trigger_iud_function_name name;
  v_trigger_iud_name name;
  v_trigger_truncate_function_name name;
  v_trigger_truncate_name name;
  v_view_hist_name name;
  v_table_changes_name name;
  v_table_at_timestamp_name name;  
begin
  call pghist.hist_object_names(v_schema, v_table_name, v_hist_schema, v_hist_table_name, v_trigger_iud_function_name, v_trigger_iud_name, v_trigger_truncate_function_name, v_trigger_truncate_name, v_view_hist_name, v_table_changes_name, v_table_at_timestamp_name);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop view if exists '||v_schema||'.'||v_view_hist_name); 
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_schema||'.'||v_table_changes_name);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_schema||'.'||v_table_at_timestamp_name);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_schema||'.'||v_trigger_iud_function_name||' cascade');
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_schema||'.'||v_trigger_truncate_function_name||' cascade');
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop table if exists '||v_hist_schema||'.'||v_hist_table_name||' cascade');
  call pghist.hist_enable_parent(v_schema, v_table_name);
end; $$;

create or replace procedure pghist.hist_disable(table_name name) security definer language plpgsql as $$
begin
  call pghist.hist_disable(current_schema(), table_name);
end; $$;

create or replace function pghist.event_fn_ddl_command() returns event_trigger security definer as $$
declare
  rec record;
begin
  for rec in select * from ( 
    select schema_name,relname table_name
      from pg_event_trigger_ddl_commands() e
      join pg_class c on c.oid=e.objid
      where object_type in ('table','table column')
    ) t where pghist.hist_exists(schema_name,table_name) 
  loop
    call pghist.hist_enable(rec.schema_name, rec.table_name);
  end loop;
  for rec in select * from ( 
    select schema_name schema_name,relname table_name
      from pg_event_trigger_ddl_commands() e
      join pg_index i on i.indexrelid=e.objid
      join pg_class c on c.oid=i.indrelid
      where object_type in ('index')
    ) t where pghist.hist_exists(schema_name,table_name) 
  loop
    call pghist.hist_enable(rec.schema_name, rec.table_name);
  end loop;	
end;
$$ language plpgsql;

create or replace function pghist.event_fn_drop_table() returns event_trigger security definer as $$
declare
  rec record;
begin  	
  for rec in 
    select schema_name,object_name table_name 
     from pg_event_trigger_dropped_objects()
     where object_type='table' and pghist.hist_exists(schema_name,object_name)
  loop
    call pghist.hist_disable(rec.schema_name, rec.table_name);
  end loop;	
end;
$$ language plpgsql;

do $$ begin
  if not exists (select 1 from pg_event_trigger where evtname='pghist_event_tg_ddl_command') then
    create event trigger pghist_event_tg_ddl_command on ddl_command_end when tag in ('ALTER TABLE','CREATE INDEX','COMMENT') execute procedure pghist.event_fn_ddl_command();
  end if;
  if not exists (select 1 from pg_event_trigger where evtname='pghist_event_tg_drop_table') then
    create event trigger pghist_event_tg_drop_table on sql_drop when tag in ('DROP TABLE','DROP SCHEMA') execute function pghist.event_fn_drop_table();
  end if; 
end $$;



create or replace procedure pghist.hist_enable(schema name, table_name name, master_table_schema name default null, master_table_name name default null) security definer language plpgsql as $body$
declare 
  v_schema name := lower(schema);
  v_table_name name := lower(table_name);
  v_table_oid oid := (v_schema||'.'||v_table_name)::regclass::oid;
  v_table_comment varchar := coalesce(quote_literal(col_description(v_table_oid, 0)),'null');
  v_table_owner name; 
  v_children name[][]; 
  v_columns_name name[];
  v_columns_type name[];
  v_columns_pkey name[];
  v_columns_fkey_master_table name[];
  v_columns_immutable name[];
  v_columns_old name[];
  v_columns_expr_comment varchar[];
  v_columns_expr_value_new_desc varchar[];
  v_columns_expr_value_old_desc varchar[];
  v_row_desc_expr varchar; 
  --
  v_hist_schema name;  
  v_hist_table_name name;
  v_hist_table_oid oid;
  --
  v_trigger_iud_function_name name;
  v_trigger_iud_prefix name;
  v_trigger_truncate_function_name name;
  v_trigger_truncate_name name;
  --
  v_view_hist_name name;  
  v_table_changes_name name;
  v_table_at_timestamp_name name;
  --
  v_sql text;
  v_sql_part text; 
  v_sql_condition text;
  v_sql_hist_to_row text;
  v_col name;
  v_expression varchar;
  v_operation varchar;
  v_type_convert varchar;
  v_i int; 
  v_rec record; 
  v_newline char := E'\n';
begin
  select relowner::regrole::name into v_table_owner from pg_class where oid=v_table_oid;
  call pghist.hist_object_names(v_schema, v_table_name, v_hist_schema, v_hist_table_name, v_trigger_iud_function_name, v_trigger_iud_prefix, v_trigger_truncate_function_name, v_trigger_truncate_name, v_view_hist_name, v_table_changes_name, v_table_at_timestamp_name);  
  select array_agg(attname order by attnum),
         array_agg(case when tn.nspname!='pg_catalog' then tn.nspname||'.' else '' end || typname order by attnum), 
         array_agg(' v_change.column_comment := '||coalesce(quote_literal(col_description(attrelid,attnum)), 'null')||';' order by attnum)
    into v_columns_name, v_columns_type, v_columns_expr_comment
    from pg_attribute a
    join pg_type t on t.oid=a.atttypid
    join pg_namespace tn on tn.oid=t.typnamespace
    where attrelid=v_table_oid and attnum>0 and not attisdropped;
  select coalesce(array_agg(attname order by col_pos),array[]::name[])
    into v_columns_pkey
    from unnest( (select conkey from pg_constraint where conrelid=v_table_oid and contype='p') ) with ordinality c(col_num,col_pos)
    join pg_attribute a on attrelid=v_table_oid and attnum = col_num;
  if v_columns_pkey is null then
    raise exception 'PGHIST-002 Table does not have primary key';
  end if;       
  select array_agg(array[child_schema,child_table_name] order by child_schema,child_table_name)
    into v_children
    from (
      select c.relnamespace::regnamespace::name child_schema, c.relname child_table_name 
        from pg_inherits i
        join pg_class c on c.oid=i.inhrelid
        where i.inhparent = v_table_oid
    ) c
    where pghist.hist_exists(child_schema,child_table_name);
  --
  select oid into v_hist_table_oid from pg_class where relnamespace::regnamespace::name=v_hist_schema and relname=v_hist_table_name;
  if v_hist_table_oid is null then
    v_columns_immutable := v_columns_pkey;
    if master_table_schema is not null and master_table_name is not null then
      select (
        select array_agg(attname order by col_pos)
          from unnest(conkey) with ordinality col(col_num,col_pos)
          join pg_attribute on attrelid=conrelid and attnum=col_num
        )  
        into v_columns_fkey_master_table
        from pg_constraint   
        where contype='f' and conrelid=v_table_oid and confrelid=(master_table_schema||'.'||master_table_name)::regclass::oid
        order by conkey[1]
        limit 1;
      if v_columns_fkey_master_table is null then
        raise exception 'PGHIST-003 Not found foreign key on master table';
      end if;
      foreach v_col in array v_columns_fkey_master_table loop
        if not v_col=any(v_columns_immutable) then
          v_columns_immutable := v_columns_immutable||v_col;
        end if;
      end loop;
    else
      v_columns_fkey_master_table := null;      
 	end if;
    v_sql :=
      'create table '||v_hist_schema||'.'||v_hist_table_name||' ('||v_newline||
      '  hist_statement_id bigint not null, -- references '||v_hist_schema||'.hist_statement(id) on delete cascade,'||v_newline||
      '  hist_row_num int not null,'||v_newline||             
      '  hist_update_columns name[],'||v_newline;
    foreach v_col in array v_columns_name loop     
      v_sql := v_sql||'  '||v_col||case when v_col=any(v_columns_immutable) then '' else '_old' end||' '||v_columns_type[array_position(v_columns_name,v_col)]||case when v_col=any(v_columns_immutable) then ' not null' else '' end||','||v_newline;
    end loop;
    v_sql := v_sql||
     '  primary key ('||pghist.hist_columns_to_text(v_columns_pkey)||',hist_statement_id)'||v_newline||    
     ')';
    call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
    call pghist.hist_execute_sql(v_schema, v_table_name, 'comment on column '||v_hist_schema||'.'||v_hist_table_name||'.hist_update_columns is '||quote_literal(v_columns_immutable));
    call pghist.hist_execute_sql(v_schema, v_table_name, 'lock table '||v_hist_schema||'.'||v_hist_table_name||' in exclusive mode');
    v_sql :=
      'insert into '||v_hist_schema||'.'||v_hist_table_name||' (hist_statement_id,hist_row_num,'||pghist.hist_columns_to_text(v_columns_immutable)||')'||v_newline||   
      '  select '||pghist.hist_statement_id('HIST_ENABLE')||',row_number() over (),'||pghist.hist_columns_to_text(v_columns_immutable)||' from only '||v_schema||'.'||v_table_name;
    call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
    if v_columns_fkey_master_table is not null then
      call pghist.hist_execute_sql(v_schema, v_table_name, 'create index on '||v_hist_schema||'.'||v_hist_table_name||'('||pghist.hist_columns_to_text(v_columns_fkey_master_table)||')');
    end if; 
    call pghist.hist_execute_sql(v_schema, v_table_name, 'grant select on '||v_hist_schema||'.hist_transaction,'||v_hist_schema||'.'||v_hist_table_name||' to '||v_table_owner);  
    v_hist_table_oid := (v_hist_schema||'.'||v_hist_table_name)::regclass::oid;
  else
    v_columns_immutable := col_description(v_hist_table_oid, 3);
    for v_rec in
	  select attname column_name
	    from pg_attribute
	    where attrelid=v_hist_table_oid and not attname=any(v_columns_immutable) and attnum>3 and not attisdropped
          and (attname,atttypid) not in (
            select attname||'_old',atttypid from pg_attribute where attrelid=v_table_oid and attnum>0 and not attisdropped
          )
    loop
      call pghist.hist_execute_sql(v_schema, v_table_name, 'drop view if exists '||v_schema||'.'||v_view_hist_name);
      call pghist.hist_execute_sql(v_schema, v_table_name, 'alter table '||v_hist_schema||'.'||v_hist_table_name||' drop column '||v_rec.column_name);
    end loop;
    for v_rec in   
      select attname column_name, typname column_type, pg_get_expr(d.adbin,d.adrelid) default_value
	    from pg_attribute a
	    join pg_type t on t.oid=a.atttypid
	    left join pg_attrdef d on d.adrelid=a.attrelid and d.adnum=a.attnum 
	    where attrelid=v_table_oid and not attname=any(v_columns_immutable) and attnum>0 and not attisdropped
          and attname||'_old' not in (select attname from pg_attribute where attrelid=v_hist_table_oid and attnum>0 and not attisdropped)                   
        order by attnum          
    loop
      call pghist.hist_execute_sql(v_schema, v_table_name, 'alter table '||v_hist_schema||'.'||v_hist_table_name||' add column '||v_rec.column_name||'_old '||v_rec.column_type);
      if v_rec.default_value is not null then
        call pghist.hist_execute_sql(v_schema, v_table_name, 'lock table '||v_hist_schema||'.'||v_hist_table_name||' in exclusive mode');
        v_sql :=
          'insert into '||v_hist_schema||'.'||v_hist_table_name||' (hist_statement_id,hist_row_num,hist_update_columns,'||pghist.hist_columns_to_text(v_columns_immutable)||')'||v_newline||   
          '  select '||pghist.hist_statement_id('UPDATE')||',row_number() over (),array['||pg_catalog.quote_literal(v_rec.column_name)||'],'||pghist.hist_columns_to_text(v_columns_immutable)||' from only '||v_schema||'.'||v_table_name;
        call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);         
      end if; 
    end loop;
  end if;
  select array_agg(col) into v_columns_old from unnest(v_columns_name) col where not col = any(v_columns_immutable); 
  for v_i in 1..array_length(v_columns_name,1) loop
    v_expression := pghist.hist_expression_value_desc_current(v_schema, v_table_name, v_columns_name[v_i]);
    if v_expression is not null then
      v_sql_part := ' execute $$ select ('||v_expression||') $$ into v_change.value_';
      v_columns_expr_value_new_desc[v_i] := v_sql_part||'new_desc using v_row.'||v_columns_name[v_i]||';';
      v_columns_expr_value_old_desc[v_i] := v_sql_part||'old_desc using v_row.'||v_columns_name[v_i]||';';     
    else 
      v_columns_expr_value_new_desc[v_i] := ' v_change.value_new_desc := v_change.value_new;';
      v_columns_expr_value_old_desc[v_i] := ' v_change.value_old_desc := v_change.value_old;';     
    end if;
  end loop;
  v_row_desc_expr := 'execute $$ select ('||pghist.hist_expression_row_desc_current(v_schema, v_table_name)||') $$ into v_change.row_desc using v_row;';
  --
  v_sql := 
    'create or replace function '||v_hist_schema||'.'||v_trigger_iud_function_name||'() returns trigger language plpgsql security definer as $func$'||v_newline||
    'declare '||v_newline||
    '  v_statement_id bigint = pghist.hist_statement_id(tg_op);'||v_newline||
    'begin '||v_newline||
    '  if tg_op=''INSERT'' then'||v_newline||
    '    insert into '||v_hist_schema||'.'||v_hist_table_name||' (hist_statement_id,hist_row_num,'||pghist.hist_columns_to_text(v_columns_immutable)||')'||v_newline||
    '      select v_statement_id,row_number() over (),'||pghist.hist_columns_to_text(v_columns_immutable)||v_newline||
    '        from rows_new;'||v_newline||
    '    return null;'||v_newline||
    '  end if;'||v_newline||    
    '  if tg_op=''UPDATE'' then'||v_newline||    
    '    if exists ('||v_newline||
    '      select from rows_old o'||v_newline||
    '        left join rows_new n on '||pghist.hist_columns_to_text(v_columns_immutable,$$ 'o.'||col||'=n.'||col $$,' and ')||v_newline||
    '        where n.'||v_columns_pkey[1]||' is null'||v_newline||
    '    ) then '||v_newline||
    '      raise exception ''PGHIST-001 Update column(s) '||pghist.hist_columns_to_text(v_columns_immutable)||' of table '||v_schema||'.'||v_table_name||' is not allowed'';'||v_newline||
    '    end if;'||v_newline|| 
    '    insert into '||v_hist_schema||'.'||v_hist_table_name||' (hist_statement_id,hist_row_num,hist_update_columns,'||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_old,$$','||col||'_old'$$,'')||')'||v_newline||
    '      select v_statement_id,'||v_newline||
    '             hist_row_num,'||v_newline||
    '             array[]::name[]'||v_newline;
  v_sql_part := ''||pghist.hist_columns_to_text(v_columns_immutable,''','''||v_newline||'''             o.''||col','');
  foreach v_col in array v_columns_old loop  
    v_type_convert := case when v_columns_type[array_position(v_columns_name, v_col)] in ('json','_json','xml','_xml') then '::text' else '' end;
    v_sql_condition := '(o.'||v_col||v_type_convert||'!=n.'||v_col||v_type_convert||') or (o.'||v_col||' is null and n.'||v_col||' is not null) or (o.'||v_col||' is not null and n.'||v_col||' is null)';
    v_sql := v_sql||'               ||(case when '||v_sql_condition||' then ''{'||v_col||'}'' end)::name[]'||case when array_position(v_columns_old,v_col)!=array_length(v_columns_old,1) then v_newline else '' end;
    v_sql_part := v_sql_part||','||v_newline||'             case when '||v_sql_condition||' then o.'||v_col||' end' end;
  end loop;
  v_sql := v_sql||v_sql_part||v_newline||
    '       from (select row_number() over () hist_row_num,* from rows_old) o'||v_newline||
    '       join rows_new n on '||pghist.hist_columns_to_text(v_columns_pkey,$$ 'o.'||col||'=n.'||col $$,' and ')||';'||v_newline||
    '    return null;'||v_newline|| 
    '  end if;'||v_newline||
    '  if tg_op=''DELETE'' then'||v_newline||
    '    insert into '||v_hist_schema||'.'||v_hist_table_name||' (hist_statement_id,hist_row_num,'||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_old,$$ ','||col||'_old' $$,'')||')'||v_newline||
    '      select v_statement_id,row_number() over (),'||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_old,$$ ','||col $$,'')||v_newline||
    '        from rows_old;'||v_newline||
    '    return null;'||v_newline||
    '  end if;'||v_newline||    
    '  return null;'||v_newline||
    'end;'||v_newline||
    '$func$';   
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'grant execute on function '||v_hist_schema||'.'||v_trigger_iud_function_name||' to '||v_table_owner); 
  foreach v_operation in array array['insert','update','delete'] loop
    if not exists (select from pg_trigger where tgrelid=v_table_oid and tgname=v_trigger_iud_prefix||v_operation) then
      v_sql :=
        'create trigger '||v_trigger_iud_prefix||v_operation||
        '  after '||v_operation||' on '||v_schema||'.'||v_table_name||
        '  referencing '||
        '     '||case when v_operation in ('insert','update') then 'new table as rows_new' else '' end||
        '     '||case when v_operation in ('update','delete') then 'old table as rows_old' else '' end||
        '  for each statement '||
        '  execute procedure '||v_hist_schema||'.'||v_trigger_iud_function_name||'();';        
      call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
    end if;  
  end loop;
  --
  v_sql := 
   'create or replace function '||v_hist_schema||'.'||v_trigger_truncate_function_name||'() returns trigger language plpgsql security definer as $func$'||v_newline||
    'declare '||v_newline||
    '  v_statement_id bigint = pghist.hist_statement_id(tg_op);'||v_newline||
    'begin '||v_newline||
    '  insert into '||v_hist_schema||'.'||v_hist_table_name||' (hist_statement_id,hist_row_num,'||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_old,$$','||col||'_old'$$,'')||')'||v_newline||
    '    select v_statement_id,row_number() over (),'||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_old,$$ ','||col $$,'')||v_newline||
    '      from '||v_schema||'.'||v_table_name||';'||v_newline||
    '   return null;'||v_newline||
    'end;'||v_newline||
    '$func$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  if not exists (select 1 from pg_trigger where tgrelid=v_table_oid and tgname=v_trigger_truncate_name) then
    call pghist.hist_execute_sql(v_schema, v_table_name, 'create trigger '||v_trigger_truncate_name||' before truncate on '||v_schema||'.'||v_table_name||' execute procedure '||v_hist_schema||'.'||v_trigger_truncate_function_name||'();');
  end if;
  --
  v_sql := 
    'create or replace view '||v_schema||'.'||v_view_hist_name||' as '||v_newline||
    '  select reverse(h.hist_statement_id::varchar)::bigint hist_statement_num,hist_row_num,s.timestamp hist_timestamp,s.operation hist_operation,h.hist_update_columns,t.db_user hist_db_user,t.app_user hist_app_user,t.application_name hist_application_name,q.text hist_query_text,h.hist_statement_id,'||v_newline||
    '	      '||pghist.hist_columns_to_text(v_columns_immutable,$$ 'h.'||col $$)||pghist.hist_columns_to_text(v_columns_old,$$ ',h.'||col||'_old' $$,'')||v_newline||
    '    from '||v_hist_schema||'.'||v_hist_table_name||' h'||v_newline||
    '    join pghist.hist_statement s on s.id=h.hist_statement_id'||v_newline||
    '    join pghist.hist_query q on q.hash=s.query_hash'||v_newline||
    '    join pghist.hist_transaction t on t.id=s.transaction_id'||v_newline||
    '  order by 1,2';  
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'grant select on '||v_schema||'.'||v_view_hist_name||' to '||v_table_owner||' with grant option'); 
  --
  v_sql_hist_to_row := pghist.hist_columns_to_text(v_columns_immutable,$$ 'v_row.'||col||' := v_hist.'||col||';' $$,' ')||pghist.hist_columns_to_text(v_columns_old,$$ ' v_row.'||col||' := v_hist.'||col||'_old;' $$,''); 
  v_sql := 
    'create or replace function '||v_schema||'.'||v_table_changes_name||'(where_clause text default null, where_param anyelement default null::varchar, columns_immutable boolean default false, insert_detail boolean default false, cascade boolean default true) returns setof pghist.table_change language plpgsql security definer as $func$'||v_newline||
    'declare'||v_newline||
    '  v_row '||v_schema||'.'||v_table_name||'%rowtype;'||v_newline||    
    '  v_change pghist.table_change;'||v_newline||
    '  v_hist record;'||v_newline||
    '  v_cur_hist refcursor;'||v_newline||
    'begin'||v_newline||
    '  v_change.schema := '''||v_schema||''';'||v_newline||
    '  v_change.table_name := '''||v_table_name||''';'||v_newline||
    '  v_change.table_comment := '||v_table_comment||';'||v_newline||   
    '  open v_cur_hist for execute'||v_newline||
    '    ''select reverse(s.hist_statement_id::varchar)::bigint hist_statement_num,hist_row_num,hist_timestamp,hist_operation,hist_db_user,hist_app_user,hist_update_columns,'||v_newline||
    '            '||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_old,$$ ','||col||'_old' $$,'')||v_newline||
    '       from '||v_hist_schema||'.'||v_hist_table_name||' h'||v_newline||
    '       join (select id hist_statement_id, transaction_id hist_transaction_id, operation hist_operation,timestamp hist_timestamp from pghist.hist_statement) s on s.hist_statement_id=h.hist_statement_id'||v_newline||
    '       join (select id hist_transaction_id, db_user hist_db_user, app_user hist_app_user from pghist.hist_transaction) t on t.hist_transaction_id=s.hist_transaction_id '''||v_newline||
    '       ||coalesce(''where ''||where_clause||'' '', '''')||'||v_newline||
    '    ''union all ''||'||v_newline||
    '    ''select null,null,null,null,null,null,null,'||v_newline||
    '            '||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_old,$$ ','||col $$,'')||v_newline||
    '       from only '||v_schema||'.'||v_table_name||' '''||v_newline||
    '       ||coalesce(''where ''||where_clause||'' '', '''')||'||v_newline||
    '    ''order by '||pghist.hist_columns_to_text(v_columns_pkey)||',hist_statement_num desc'''||v_newline||
    '    using where_param;'||v_newline||
    '  loop'||v_newline||
    '    fetch v_cur_hist into v_hist;'||v_newline||
    '    exit when not found;'||v_newline||
    '    if v_hist.hist_operation is null then'||v_newline||
    '      '||v_sql_hist_to_row||v_newline||
    '      continue;'||v_newline||   
    '    end if;'||v_newline||
    '    v_change.statement_num  := v_hist.hist_statement_num;'||v_newline||    
    '    v_change.row_num        := v_hist.hist_row_num;'||v_newline||    
    '    v_change.timestamp      := v_hist.hist_timestamp;'||v_newline||
    '    v_change.operation      := v_hist.hist_operation;'||v_newline||
    '    v_change.operation_name := '||v_hist_schema||'.hist_custom_operation_name(v_change.operation);'||v_newline||
    '    v_change.db_user        := v_hist.hist_db_user;'||v_newline||
    '    v_change.db_user_name   := '||v_hist_schema||'.hist_custom_db_user_name(v_change.db_user);'||v_newline||
    '    v_change.app_user       := v_hist.hist_app_user;'||v_newline||
    '    v_change.app_user_name  := '||v_hist_schema||'.hist_custom_db_user_name(v_change.app_user);'||v_newline||
    '    if v_hist.hist_operation in (''HIST_ENABLE'',''INSERT'') then'||v_newline||
    '      v_change.value_old := null; v_change.value_old_desc := null;'||v_newline||
    '      '||v_row_desc_expr||v_newline||
    '      if insert_detail then'||v_newline;   
  for v_i in 1..array_length(v_columns_name,1) loop
    v_col := v_columns_name[v_i];
    if v_col=any(v_columns_immutable) then 
      v_sql := v_sql||'        if columns_immutable then'||v_newline;
    else    
      v_sql := v_sql||'        if v_row.'||v_col||' is not null then'||v_newline;
    end if;
    v_sql := v_sql||
      '          v_change.column_name := '||quote_literal(v_col)||'; v_change.column_num := '||v_i||';'||v_columns_expr_comment[v_i]||v_newline||
      '          v_change.value_new := v_row.'||v_col||';'||v_columns_expr_value_new_desc[v_i]||v_newline||
      '          return next v_change;'||v_newline||
      '        end if;'||v_newline;  
  end loop;
  v_sql := v_sql||
    '      else'||v_newline||
    '        v_change.column_name := null; v_change.column_num := null; v_change.column_comment := null;'||v_newline||
    '        v_change.value_new := null; v_change.value_new_desc := null;'||v_newline||    
    '        return next v_change;'||v_newline||    
    '      end if;'||v_newline||    
    '      continue;'||v_newline|| 
    '    end if;'||v_newline||
    '    if v_hist.hist_operation = ''UPDATE'' then'||v_newline||
    '      '||v_row_desc_expr||v_newline||
    '      foreach v_change.column_name in array v_hist.hist_update_columns loop'||v_newline;
  foreach v_col in array v_columns_old loop
    v_i := pg_catalog.array_position(v_columns_name, v_col);
    v_sql := v_sql||
      '        if v_change.column_name='||quote_literal(v_col)||' then'||v_newline||
      '          v_change.column_name := '||quote_literal(v_col)||'; v_change.column_num := '||v_i||';'||v_columns_expr_comment[v_i]||v_newline||
      '          v_change.value_new := v_row.'||v_col||';'||v_columns_expr_value_new_desc[v_i]||v_newline||
      '          v_row.'||v_col||' := v_hist.'||v_col||'_old;'||v_newline|| 
      '          v_change.value_old := v_row.'||v_col||';'||v_columns_expr_value_old_desc[v_i]||v_newline||      
      '          return next v_change;'||v_newline||
      '          continue;'||v_newline|| 
      '        end if;'||v_newline;
  end loop;
  v_sql := v_sql|| 
    '      end loop;'||v_newline||
    '      continue;'||v_newline|| 
    '    end if;'||v_newline||
    '    if v_hist.hist_operation in (''DELETE'',''TRUNCATE'') then'||v_newline||
    '      v_change.value_new := null; v_change.value_new_desc := null;'||v_newline||
    '      '||v_sql_hist_to_row||v_newline||
    '      '||v_row_desc_expr||v_newline;
  for v_i in 1..array_length(v_columns_name,1) loop
    v_col := v_columns_name[v_i];
    if v_col=any(v_columns_immutable) then 
      v_sql := v_sql||'      if columns_immutable then'||v_newline;
    else    
      v_sql := v_sql||'      if v_hist.'||v_col||'_old is not null then'||v_newline;
    end if;
    v_sql := v_sql||
      '        v_change.column_name := '||quote_literal(v_col)||'; v_change.column_num := '||v_i||';'||v_columns_expr_comment[v_i]||v_newline||
      '        v_change.value_old := v_row.'||v_col||';'||v_columns_expr_value_old_desc[v_i]||v_newline||      
      '        return next v_change;'||v_newline||
      '      end if;'||v_newline;  
  end loop;
  v_sql := v_sql||
    '      continue;'||v_newline|| 
    '    end if;'||v_newline||    
    '  end loop;'||v_newline||
    '  close v_cur_hist;'||v_newline;
  if v_children is not null then    
    v_sql := v_sql||'  if cascade then'||v_newline;
    for v_i in 1..array_length(v_children, 1) loop
      v_sql := v_sql||
        '    for v_change in (select * from  '||v_children[v_i][1]||'.'||v_children[v_i][2]||'_changes(where_clause, where_param, columns_immutable, insert_detail, cascade)) loop'||v_newline||
        '      return next v_change;'||v_newline||
        '    end loop;'||v_newline;
    end loop;   
    v_sql := v_sql||'  end if;'||v_newline;
  end if; 
  v_sql := v_sql||    
    'end;'||v_newline||
    '$func$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'grant execute on function '||v_schema||'.'||v_table_changes_name||' to '||v_table_owner||' with grant option'); 
  -- 
  v_sql :=
    'create or replace function '||v_schema||'.'||v_table_at_timestamp_name||'(transaction_timestamp timestamptz default current_setting(''pghist.at_timestamp'')::timestamptz, cascade boolean default true) returns setof '||v_schema||'.'||v_table_name||' security definer language plpgsql as $func$'||v_newline||
    'declare'||v_newline||
    '  v_row '||v_schema||'.'||v_table_name||'%rowtype;'||v_newline||
    '  v_hist record;'||v_newline||
    '  v_hist_not_found boolean;'||v_newline||
    '  v_cur_hist cursor for'||v_newline||
    '    select s.operation hist_operation,h.hist_statement_id,h.hist_update_columns,'||pghist.hist_columns_to_text(v_columns_immutable,'''h.''||col')||pghist.hist_columns_to_text(v_columns_old,$$',h.'||col||'_old'$$,'')||v_newline||
    '      from '||v_hist_schema||'.'||v_hist_table_name||' h'||v_newline||
    '      join pghist.hist_statement s on s.id=h.hist_statement_id'||v_newline||
    '      join pghist.hist_transaction t on t.id=s.transaction_id and t.timestamp_commit>=transaction_timestamp'||v_newline||    
    '    union all'||v_newline||
    '    select null,null,null,'||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_old,$$','||col$$,'')||v_newline||
    '      from only '||v_schema||'.'||v_table_name||v_newline||    
    '    order by id,hist_statement_id desc;'||v_newline||
    '  v_column name;'||v_newline||
    'begin'||v_newline||
    '  drop table if exists '||v_schema||'_'||v_table_at_timestamp_name||';'||v_newline||
    '  create temp table '||v_schema||'_'||v_table_at_timestamp_name||' as select * from '||v_schema||'.'||v_table_name||' limit 0;'||v_newline||
    '  open v_cur_hist;'||v_newline||
    '  loop'||v_newline||
    '    fetch v_cur_hist into v_hist;'||v_newline||
    '    v_hist_not_found := not found;'||v_newline||
    '    if (v_hist_not_found and v_row.'||v_columns_pkey[1]||' is not null) or ('||pghist.hist_columns_to_text(v_columns_pkey,$$'v_row.'||col||'!=v_hist.'||col$$,' or ')||') then'||v_newline||
    '      insert into '||v_schema||'_'||v_table_at_timestamp_name||' values (v_row.*);'||v_newline||
    '      v_row := null;'||v_newline||   
    '    end if;'||v_newline||
    '    exit when v_hist_not_found;'||v_newline||
    '    if v_hist.hist_operation is null or v_hist.hist_operation in (''DELETE'',''TRUNCATE'') then'||v_newline||
    '    '||v_sql_hist_to_row||v_newline||
    '      continue;'||v_newline||   
    '    end if;'||v_newline||
    '    if v_hist.hist_operation = ''UPDATE'' then'||v_newline||
    '      foreach v_column in array v_hist.hist_update_columns loop'||v_newline;
  foreach v_col in array v_columns_old loop
    v_sql := v_sql||
      '        if v_column='''||v_col||''' then v_row.'||v_col||':=v_hist.'||v_col||'_old; continue; end if;'||v_newline;
  end loop;
  v_sql := v_sql||   
    '      end loop;'||v_newline||
    '      continue;'||v_newline|| 
    '    end if;'||v_newline||
    '    if v_hist.hist_operation in (''INSERT'',''HIST_ENABLE'') then'||v_newline||
    '      v_row := null;'||v_newline||
    '      continue;'||v_newline||   
    '    end if;'||v_newline||
    '  end loop;'||v_newline||
    '  close v_cur_hist;'||v_newline;
  if v_children is not null then    
    v_sql := v_sql||'  if cascade then'||v_newline;
    for v_i in 1..array_length(v_children, 1) loop
      v_sql := v_sql||
        '    insert into '||v_schema||'_'||v_table_at_timestamp_name||v_newline||
        '      select '||pghist.hist_columns_to_text(v_columns_name)||v_newline||
        '         from '||v_children[v_i][1]||'.'||v_children[v_i][2]||'_at_timestamp(transaction_timestamp, cascade);'||v_newline;
    end loop;   
    v_sql := v_sql||'  end if;'||v_newline;
  end if; 
  select 
      string_agg( 
        '  create index on '||v_schema||'_'||v_table_at_timestamp_name||' ('|| 
        (select string_agg(attname, ',' order by col_pos)
           from unnest(indkey) with ordinality col(col_num,col_pos)
           join pg_attribute on attrelid=indrelid and attnum=col_num
        )||
  	    '); -- '||relname||v_newline
  	   , '' order by relname)    
  	into v_sql_part   
    from pg_index
    join pg_class on pg_class.oid=indexrelid
    where indrelid=v_table_oid and 0!=any(indkey);   
  v_sql := v_sql||v_sql_part||   
    '  for v_row in (select * from '||v_schema||'_'||v_table_at_timestamp_name||') loop'||v_newline||
    '    return next v_row;'||v_newline||    
    '  end loop;'||v_newline||
    'end;'||v_newline||
    '$func$';  
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'grant execute on function '||v_schema||'.'||v_table_at_timestamp_name||' to '||v_table_owner||' with grant option'); 
  call pghist.hist_enable_parent(v_schema, v_table_name);
end; $body$;

create or replace procedure pghist.hist_expression_row_desc(schema name, table_name name, expression varchar) language plpgsql as $$
begin
  call pghist.hist_execute_sql(schema, table_name, 'comment on table pghist.'||pghist.hist_table_name(schema,table_name)||' is '||quote_nullable(expression));
  call pghist.hist_enable(schema, table_name); 
end; $$;

create or replace function pghist.hist_expression_row_desc_default(schema name, table_name name) returns varchar language plpgsql as $$
declare
  v_table_oid oid := (schema||'.'||table_name)::regclass::oid;
begin
  return (
    select quote_literal(coalesce(col_description(v_table_oid, 0), table_name)||' #')||'||'||string_agg('$1.'||attname, '||'',''||' order by col_pos) expr
      from unnest( (select conkey from pg_constraint where conrelid=v_table_oid and contype='p') ) with ordinality c(col_num,col_pos)
      join pg_attribute a on attrelid=v_table_oid and attnum = col_num
  );	
end; $$;

create or replace function pghist.hist_expression_row_desc_current(schema name, table_name name) returns varchar language plpgsql as $$
begin
  return coalesce(col_description(('pghist.'||pghist.hist_table_name(schema,table_name))::regclass::oid,0),pghist.hist_expression_row_desc_default(schema,table_name));
end; $$;

create or replace procedure pghist.hist_expression_value_desc(schema name, table_name name, column_name name, expression varchar) language plpgsql as $$
begin
  call pghist.hist_execute_sql(schema, table_name, 'comment on column pghist.'||pghist.hist_table_name(schema,table_name)||'.'||lower(pghist.hist_column_name(schema,table_name,column_name))||' is '||coalesce(quote_literal(expression),'null'));
  call pghist.hist_enable(schema, table_name); 
end; $$;

create or replace function pghist.hist_expression_value_desc_default(schema name, table_name name, column_name name) returns varchar language plpgsql as $$
declare
  v_table_oid oid := (schema||'.'||table_name)::regclass::oid;
  v_column_name name := lower(column_name);
begin
  return (
    select 'select '||f_text.attname||' from '||f_tab.relnamespace::regnamespace::name||'.'||f_tab.relname||' where '||f_key.attname||'=$1'  
      from pg_constraint c
      join pg_attribute ca on ca.attrelid=c.conrelid and ca.attnum=c.conkey[1] and ca.attname=v_column_name 
      join pg_class f_tab on f_tab.oid=c.confrelid
      join pg_attribute f_key on f_key.attrelid=c.confrelid and f_key.attnum=c.confkey[1]
      join pg_attribute f_text on f_text.attrelid=c.confrelid and f_text.attnum!=f_key.attnum 
      join pg_type t on t.oid=f_text.atttypid and t.typcategory='S'
      where c.conrelid=v_table_oid and c.contype='f' and array_length(c.conkey,1)=1
      order by f_text.attnum
      limit 1
  );
end; $$;

create or replace function pghist.hist_expression_value_desc_current(schema name, table_name name, column_name name) returns varchar language plpgsql as $$
begin
  return (
    select coalesce(col_description(attrelid,attnum), pghist.hist_expression_value_desc_default(schema, table_name, column_name))
      from pg_attribute
      where attrelid=('pghist.'||pghist.hist_table_name(schema,table_name))::regclass::oid and attname=lower(pghist.hist_column_name(schema,table_name,column_name))
  );	
end; $$;

do $block$ begin
  if to_regproc('pghist.hist_custom_operation_name') is null then
    create or replace function pghist.hist_custom_operation_name(operation varchar) returns varchar language plpgsql as $$
    begin 
      return case
    	when operation = 'HIST_ENABLE' then 'History start'
    	when operation = 'INSERT'      then 'Creation'
    	when operation = 'UPDATE'      then 'Modification'
    	when operation = 'DELETE'      then 'Deletion'
    	when operation = 'TRUNCATE'    then 'Cleaning'
      end;
    end; $$;      
  end if;
  if to_regproc('pghist.hist_custom_db_user_name') is null then
    create or replace function pghist.hist_custom_db_user_name(db_user name) returns varchar language plpgsql as $$
    begin 
      return db_user;
    end; $$;      
  end if;
  if to_regproc('pghist.hist_custom_app_user_name') is null then
    create or replace function pghist.hist_custom_app_user_name(app_user name) returns varchar language plpgsql as $$
    begin 
      return app_user;
    end; $$;      
  end if;
  if to_regproc('pghist.hist_custom_app_user') is null then
    create or replace function pghist.hist_custom_app_user() returns varchar language plpgsql as $$ begin return current_setting('app.user', true); end; $$;      
  end if;
  if to_regproc('pghist.hist_custom_app_client_addr') is null then
    create or replace function pghist.hist_custom_app_client_addr() returns inet language plpgsql as $$ begin return current_setting('app.client_addr', true)::inet; end; $$;      
  end if;
  if to_regproc('pghist.hist_custom_app_client_hostname') is null then
    create or replace function pghist.hist_custom_app_client_hostname() returns varchar language plpgsql as $$ begin return current_setting('app.client_hostname', true); end; $$;      
  end if;
end $block$;



