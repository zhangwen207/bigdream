drop table tb_stamp; 
CREATE TABLE tb_stamp 
(
TableName varchar(50) NOT NULL,
TStamp datetime NOT NULL 
);

drop table score; 
CREATE TABLE score 
(
clname varchar(50) NOT NULL,
buy int,
sell int,
profit float(53),
ratio float(53),
date datetime NOT NULL
);
