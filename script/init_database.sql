================================
#create databse
#drop and recreate database
IF EXISTS (SELECT 1 from sys.databases WHERE name='Datawarehouse_new')
BEGIN
ALTER DATABASE Datawarehouse_new SET single_user WITH ROLLBACK IMMEDIATE;
DROP DATABASE DATAWAREHOUSE_new;
END;
go
---Create the database 'datawarehouse_new
CREATE DATABASE Datawarehouse_new;
go
USE DATAWAREHOUSE_NEW;
create schema bronze;
  go
  create silver;
  go
  create gold;
  go
