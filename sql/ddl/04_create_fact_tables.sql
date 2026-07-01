-- =====================================================================
-- 04_create_fact_tables.sql
-- Huge, high-volume transactional tables (millions of rows).
-- Indexes are intentionally minimal (PK + ModifiedDate watermark + FK
-- lookups) to keep bulk-load performance reasonable; this mirrors a
-- typical OLTP source system that a migration project would pull from.
-- =====================================================================
USE MociSourceDB;
GO

IF OBJECT_ID(N'sales.order_items', N'U') IS NOT NULL DROP TABLE sales.order_items;
IF OBJECT_ID(N'sales.orders', N'U') IS NOT NULL DROP TABLE sales.orders;
GO

CREATE TABLE sales.orders
(
    OrderID       BIGINT IDENTITY(1,1) NOT NULL,
    CustomerID    INT           NOT NULL,
    StoreID       INT           NOT NULL,
    EmployeeID    INT           NOT NULL,
    OrderDate     DATETIME2(3)  NOT NULL,
    Status        VARCHAR(20)   NOT NULL,
    TotalAmount   DECIMAL(12,2) NOT NULL,
    CreatedDate   DATETIME2(3)  NOT NULL CONSTRAINT DF_orders_CreatedDate DEFAULT SYSUTCDATETIME(),
    ModifiedDate  DATETIME2(3)  NOT NULL CONSTRAINT DF_orders_ModifiedDate DEFAULT SYSUTCDATETIME(),
    RowVersion    ROWVERSION,
    CONSTRAINT PK_orders PRIMARY KEY CLUSTERED (OrderID),
    CONSTRAINT FK_orders_customers FOREIGN KEY (CustomerID) REFERENCES sales.customers (CustomerID),
    CONSTRAINT FK_orders_stores FOREIGN KEY (StoreID) REFERENCES sales.stores (StoreID),
    CONSTRAINT FK_orders_employees FOREIGN KEY (EmployeeID) REFERENCES sales.employees (EmployeeID)
);
GO
CREATE NONCLUSTERED INDEX IX_orders_ModifiedDate ON sales.orders (ModifiedDate);
CREATE NONCLUSTERED INDEX IX_orders_CustomerID ON sales.orders (CustomerID);
GO

CREATE TABLE sales.order_items
(
    OrderItemID   BIGINT IDENTITY(1,1) NOT NULL,
    OrderID       BIGINT        NOT NULL,
    ProductID     INT           NOT NULL,
    Quantity      INT           NOT NULL,
    UnitPrice     DECIMAL(10,2) NOT NULL,
    LineTotal     DECIMAL(12,2) NOT NULL,
    CreatedDate   DATETIME2(3)  NOT NULL CONSTRAINT DF_order_items_CreatedDate DEFAULT SYSUTCDATETIME(),
    ModifiedDate  DATETIME2(3)  NOT NULL CONSTRAINT DF_order_items_ModifiedDate DEFAULT SYSUTCDATETIME(),
    RowVersion    ROWVERSION,
    CONSTRAINT PK_order_items PRIMARY KEY CLUSTERED (OrderItemID),
    CONSTRAINT FK_order_items_orders FOREIGN KEY (OrderID) REFERENCES sales.orders (OrderID),
    CONSTRAINT FK_order_items_products FOREIGN KEY (ProductID) REFERENCES sales.products (ProductID)
);
GO
CREATE NONCLUSTERED INDEX IX_order_items_ModifiedDate ON sales.order_items (ModifiedDate);
CREATE NONCLUSTERED INDEX IX_order_items_OrderID ON sales.order_items (OrderID);
GO
