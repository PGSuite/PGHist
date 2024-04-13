do $$
declare
  v_rec record;
begin
  if to_regnamespace('pghist') is null then	
    return;
  end if;	
  for v_rec in
    select relnamespace::regnamespace::name schema, relname table_name
      from pg_class
      where relkind='r' and pghist.hist_exists(relnamespace::regnamespace::name,relname)
  loop
    call pghist.hist_disable(v_rec.schema,v_rec.table_name);
  end loop;
end; $$;

drop event trigger if exists pghist_event_tg_ddl_command;
drop event trigger if exists pghist_event_tg_drop_table;

drop schema if exists pghist cascade;

