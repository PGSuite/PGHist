-- Создаем таблицу
create table example(
  id int primary key,
  name varchar(20),
  number numeric(10,2),
  date date
);

-- Включаем ведение истории
call pghist.hist_enable('example');

-- Изменяем данные 
insert into example values (1, 'Пример', 10, current_date);
update example set number=20, date=date-1;

-- Получаем лог изменений по строкам
select * from example_hist;

-- Получаем изменения по полям
select * from example_changes();

-- Получаем таблицу в прошлом
select * from example_at_timestamp(now()-interval '10 second');

-- drop table example cascade;