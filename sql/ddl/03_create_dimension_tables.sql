-- =====================================================================
-- 03_create_dimension_tables.sql
-- Small "dimension-like" reference tables (thousands of rows).
-- Every table carries CreatedDate/ModifiedDate so the future Bronze
-- ingestion job can use ModifiedDate as an incremental watermark column.
-- =====================================================================
USE MociSourceDB;
GO

-- Fact tables (created in 04) hold FKs back to these dimension tables, and
-- sales.employees holds an FK to sales.stores, so on a re-run they must all
-- be dropped first (in dependency order), or DROP TABLE below fails with a
-- FOREIGN KEY reference error.
IF OBJECT_ID(N'sales.order_items', N'U') IS NOT NULL DROP TABLE sales.order_items;
IF OBJECT_ID(N'sales.orders', N'U') IS NOT NULL DROP TABLE sales.orders;
IF OBJECT_ID(N'sales.employees', N'U') IS NOT NULL DROP TABLE sales.employees;
GO

IF OBJECT_ID(N'sales.stores', N'U') IS NOT NULL DROP TABLE sales.stores;
GO
CREATE TABLE sales.stores
(
    StoreID       INT IDENTITY(1,1) NOT NULL,
    StoreCode     VARCHAR(10)       NOT NULL,
    StoreName     VARCHAR(150)      NOT NULL,
    Region        VARCHAR(100)      NOT NULL,
    Country       VARCHAR(100)      NOT NULL,
    OpenDate      DATE              NOT NULL,
    CreatedDate   DATETIME2(3)      NOT NULL CONSTRAINT DF_stores_CreatedDate DEFAULT SYSUTCDATETIME(),
    ModifiedDate  DATETIME2(3)      NOT NULL CONSTRAINT DF_stores_ModifiedDate DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_stores PRIMARY KEY CLUSTERED (StoreID)
);
GO
CREATE NONCLUSTERED INDEX IX_stores_ModifiedDate ON sales.stores (ModifiedDate);
GO

IF OBJECT_ID(N'sales.products', N'U') IS NOT NULL DROP TABLE sales.products;
GO
CREATE TABLE sales.products
(
    ProductID     INT IDENTITY(1,1) NOT NULL,
    ProductCode   VARCHAR(20)       NOT NULL,
    ProductName   VARCHAR(200)      NOT NULL,
    Category      VARCHAR(50)       NOT NULL,
    UnitPrice     DECIMAL(10,2)     NOT NULL,
    CreatedDate   DATETIME2(3)      NOT NULL CONSTRAINT DF_products_CreatedDate DEFAULT SYSUTCDATETIME(),
    ModifiedDate  DATETIME2(3)      NOT NULL CONSTRAINT DF_products_ModifiedDate DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_products PRIMARY KEY CLUSTERED (ProductID)
);
GO
CREATE NONCLUSTERED INDEX IX_products_ModifiedDate ON sales.products (ModifiedDate);
GO

IF OBJECT_ID(N'sales.customers', N'U') IS NOT NULL DROP TABLE sales.customers;
GO
CREATE TABLE sales.customers
(
    CustomerID    INT IDENTITY(1,1) NOT NULL,
    FirstName     VARCHAR(50)       NOT NULL,
    LastName      VARCHAR(50)       NOT NULL,
    Email         VARCHAR(150)      NOT NULL,
    Phone         VARCHAR(30)       NULL,
    Address       VARCHAR(200)      NULL,
    City          VARCHAR(100)      NULL,
    Country       VARCHAR(100)      NOT NULL,
    CreatedDate   DATETIME2(3)      NOT NULL CONSTRAINT DF_customers_CreatedDate DEFAULT SYSUTCDATETIME(),
    ModifiedDate  DATETIME2(3)      NOT NULL CONSTRAINT DF_customers_ModifiedDate DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_customers PRIMARY KEY CLUSTERED (CustomerID)
);
GO
CREATE NONCLUSTERED INDEX IX_customers_ModifiedDate ON sales.customers (ModifiedDate);
GO

CREATE TABLE sales.employees
(
    EmployeeID    INT IDENTITY(1,1) NOT NULL,
    StoreID       INT               NOT NULL,
    FullName      VARCHAR(150)      NOT NULL,
    JobTitle      VARCHAR(100)      NOT NULL,
    HireDate      DATE              NOT NULL,
    CreatedDate   DATETIME2(3)      NOT NULL CONSTRAINT DF_employees_CreatedDate DEFAULT SYSUTCDATETIME(),
    ModifiedDate  DATETIME2(3)      NOT NULL CONSTRAINT DF_employees_ModifiedDate DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_employees PRIMARY KEY CLUSTERED (EmployeeID),
    CONSTRAINT FK_employees_stores FOREIGN KEY (StoreID) REFERENCES sales.stores (StoreID)
);
GO
CREATE NONCLUSTERED INDEX IX_employees_ModifiedDate ON sales.employees (ModifiedDate);
CREATE NONCLUSTERED INDEX IX_employees_StoreID ON sales.employees (StoreID);
GO
