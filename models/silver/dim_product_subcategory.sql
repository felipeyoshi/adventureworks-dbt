with 
subcategories as ( select * from {{ ref('DimProductSubcategory') }}) ,

base as (
    select
        ProductSubcategoryKey,
        ProductSubcategoryAlternateKey,
        EnglishProductSubcategoryName,
        SpanishProductSubcategoryName,
        FrenchProductSubcategoryName,
        ProductCategoryKey
    from subcategories
    where ProductSubcategoryKey is not null
)

select * from base