with 
geo as ( select * from {{ ref('DimGeography') }}) ,

base as (
    select
        GeographyKey,
        City,
        StateProvinceCode,
        StateProvinceName,
        CountryRegionCode,
        EnglishCountryRegionName,
        SpanishCountryRegionName,
        FrenchCountryRegionName,
        PostalCode,
        SalesTerritoryKey,
        IpAddressLocator
    from geo
    where GeographyKey is not null
)

select * from base