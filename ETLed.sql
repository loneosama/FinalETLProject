/*
Navicat SQL Server Data Transfer

Source Server         : SQL
Source Server Version : 130000
Source Host           : 127.0.0.1:1433
Source Database       : ETLed
Source Schema         : ETLed

Target Server Type    : SQL Server
Target Server Version : 130000
File Encoding         : 65001

Date: 2017-01-02 20:11:12
*/


-- ----------------------------
-- Table structure for Customer
-- ----------------------------
DROP TABLE [ETLed].[Customer]
GO
CREATE TABLE [ETLed].[Customer] (
[CustomerID] varchar(255) NOT NULL ,
[Segment] varchar(255) NULL ,
[CardStatus] varchar(255) NULL ,
[CustomerAccount] varchar(255) NULL 
)


GO

-- ----------------------------
-- Table structure for Item
-- ----------------------------
DROP TABLE [ETLed].[Item]
GO
CREATE TABLE [ETLed].[Item] (
[ItemRowID] varchar(255) NOT NULL ,
[ItemID] varchar(255) NULL ,
[Size] int NULL ,
[Price] numeric(38) NULL ,
[Department] varchar(255) NULL ,
[Category] varchar(255) NULL ,
[Color] varchar(255) NULL 
)


GO

-- ----------------------------
-- Table structure for Location
-- ----------------------------
DROP TABLE [ETLed].[Location]
GO
CREATE TABLE [ETLed].[Location] (
[RegionID] varchar(255) NOT NULL ,
[Region] varchar(255) NULL ,
[StoreID] int NULL 
)


GO

-- ----------------------------
-- Table structure for Sales
-- ----------------------------
DROP TABLE [ETLed].[Sales]
GO
CREATE TABLE [ETLed].[Sales] (
[TransactionID] int NULL ,
[TransAmt] numeric(38) NULL ,
[SoldQty] int NULL ,
[CustomerID] varchar(255) NOT NULL ,
[RegionID] varchar(255) NOT NULL ,
[Date] date NOT NULL ,
[ItemRowID] varchar(255) NULL ,
[SalesID] int NOT NULL IDENTITY(1,1) 
)


GO
DBCC CHECKIDENT(N'[ETLed].[Sales]', RESEED, 2092836)
GO

-- ----------------------------
-- Table structure for Time
-- ----------------------------
DROP TABLE [ETLed].[Time]
GO
CREATE TABLE [ETLed].[Time] (
[Date] date NOT NULL ,
[Season] varchar(255) NULL ,
[Day] int NULL ,
[Month] int NULL ,
[Year] int NULL 
)


GO

-- ----------------------------
-- Indexes structure for table Customer
-- ----------------------------

-- ----------------------------
-- Primary Key structure for table Customer
-- ----------------------------
ALTER TABLE [ETLed].[Customer] ADD PRIMARY KEY ([CustomerID])
GO

-- ----------------------------
-- Indexes structure for table Item
-- ----------------------------

-- ----------------------------
-- Primary Key structure for table Item
-- ----------------------------
ALTER TABLE [ETLed].[Item] ADD PRIMARY KEY ([ItemRowID])
GO

-- ----------------------------
-- Indexes structure for table Location
-- ----------------------------

-- ----------------------------
-- Primary Key structure for table Location
-- ----------------------------
ALTER TABLE [ETLed].[Location] ADD PRIMARY KEY ([RegionID])
GO

-- ----------------------------
-- Indexes structure for table Sales
-- ----------------------------

-- ----------------------------
-- Primary Key structure for table Sales
-- ----------------------------
ALTER TABLE [ETLed].[Sales] ADD PRIMARY KEY ([SalesID])
GO

-- ----------------------------
-- Indexes structure for table Time
-- ----------------------------

-- ----------------------------
-- Primary Key structure for table Time
-- ----------------------------
ALTER TABLE [ETLed].[Time] ADD PRIMARY KEY ([Date])
GO

-- ----------------------------
-- Foreign Key structure for table [ETLed].[Sales]
-- ----------------------------
ALTER TABLE [ETLed].[Sales] ADD FOREIGN KEY ([CustomerID]) REFERENCES [ETLed].[Customer] ([CustomerID]) ON DELETE NO ACTION ON UPDATE NO ACTION
GO
ALTER TABLE [ETLed].[Sales] ADD FOREIGN KEY ([Date]) REFERENCES [ETLed].[Time] ([Date]) ON DELETE NO ACTION ON UPDATE NO ACTION
GO
ALTER TABLE [ETLed].[Sales] ADD FOREIGN KEY ([ItemRowID]) REFERENCES [ETLed].[Item] ([ItemRowID]) ON DELETE NO ACTION ON UPDATE NO ACTION
GO
ALTER TABLE [ETLed].[Sales] ADD FOREIGN KEY ([RegionID]) REFERENCES [ETLed].[Location] ([RegionID]) ON DELETE NO ACTION ON UPDATE NO ACTION
GO
