with 
territories as ( select * from {{ ref('DimSalesTerritory') }}) ,
base as (
    select
        SalesTerritoryKey,
        SalesTerritoryAlternateKey,
        SalesTerritoryRegion,
        SalesTerritoryCountry,
        SalesTerritoryGroup
    from territories
    where SalesTerritoryKey is not null
)

select * from base