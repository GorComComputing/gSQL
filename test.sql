create table users (Id integer, UserName varchar, Login varchar, Pswd varchar, UserRole integer);
insert into users values 1, Admin, admin, pass, 2;
select Id, UserName, Login, Pswd, UserRole from users;
