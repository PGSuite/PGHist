set client_encoding = 'UTF8';

create or replace function pghist.hist_lang_ru$operation_name(operation varchar) returns varchar language plpgsql as $$
begin 
  return case
	when operation = 'HIST_ENABLE' then 'Включение истории'
	when operation = 'INSERT'      then 'Создание'
	when operation = 'UPDATE'      then 'Изменение'
	when operation = 'DELETE'      then 'Удаление'
	when operation = 'TRUNCATE'    then 'Очистка'
  end;
end; $$;

call pghist.hist_column_custom_function('operation_name', 'pghist.hist_lang_ru$operation_name');

