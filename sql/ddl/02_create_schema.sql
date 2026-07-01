-- =====================================================================
-- 02_create_schema.sql
-- Creates the "sales" schema that holds all mock source tables.
-- =====================================================================
USE MociSourceDB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'sales')
BEGIN
    EXEC('CREATE SCHEMA sales');
END
GO
