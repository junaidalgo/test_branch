/****** Object:  View [DWH].[V_DIM_TRAIT]    Script Date: 12/24/2023 8:59:22 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



--CREATE   VIEW [DWH].[V_DIM_TRAIT]
--AS
SELECT *
--[StoreCode]
--,[TraitCode]
--,[TraitDescription]
--,[EffectiveDate] 
--,FORMAT(cast([result].filepath(1)  as date),'yyyy-MM-dd') AS fiscalweekenddate
--,EDW_ACTIVITY_DATE_TIME  
FROM   OPENROWSET(
        BULK 'https://adldpwalmartdev.dfs.core.windows.net/dl-algo-edw/Walmart-US/serving/DIM_TRAIT/fiscalweekenddate=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
where effectivedate <> fiscalweekenddate
GO




select top 5 * from [DWH].[V_DIM_TRAIT];

-- This is auto-generated code
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://adldpwalmartdev.dfs.core.windows.net/dl-algo-edw/Walmart-US/serving/DIM_TRAIT/fiscalweekenddate=2023-12-01/**',
        FORMAT = 'PARQUET'
    ) AS [result]--25601

--
-- This is auto-generated code
SELECT
    count(*)
FROM
    OPENROWSET(
        BULK 'https://adldpwalmartdev.dfs.core.windows.net/dl-algo-edw/Walmart-US/serving/DIM_TRAIT/**',
        FORMAT = 'PARQUET'
    ) AS [result]--3198084


--BATCH_ID,Storecode,TraitCode,TraitDescription,EffectiveDate,EDW_ACTIVITY


select * from [ct].[V_DIM_DATE];