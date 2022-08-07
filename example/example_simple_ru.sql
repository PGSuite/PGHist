-- Создаем таблицу
create table example(
  id int primary key,
  name varchar(20),
  number numeric(10,2),
  date_ date
);

-- Включаем ведение истории
call pghist.hist_enable('example');

-- Изменяем данные 
insert into example values (1, 'Пример', 10, current_date);
update example set number=20, date_=date_-1;

-- Получаем таблицу в прошлом
select * from example_at_timestamp(now()-interval '10 second');

-- Получаем изменения по полям
select * from example_changes();

-- drop table example cascade;