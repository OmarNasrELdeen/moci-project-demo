-- =====================================================================
-- 01_create_database.sql
-- Creates the mock source database used for the SQL Server -> Databricks
-- migration demo. SIMPLE recovery model is used on purpose: this is a
-- throwaway mock-data source, not a production database, and it keeps
-- the transaction log small while bulk-loading millions of rows.
-- =====================================================================
IF DB_ID(N'MociSourceDB') IS NULL
BEGIN
    CREATE DATABASE MociSourceDB;
END
GO

ALTER DATABASE MociSourceDB SET RECOVERY SIMPLE;
GO
