drop table tb_stamp; 
CREATE TABLE tb_stamp 
(
TableName varchar(50) NOT NULL,
TStamp datetime NOT NULL 
);

drop table score; 
CREATE TABLE score 
(
date datetime NOT NULL,
code varchar(10) NOT NULL,
clname varchar(50) NOT NULL,
buy int,
sell int,
profit float(53),
ratio float(53),
buymark int,
sellmark int
);
