with 
categories as ( select * from {{ ref('DimProductCategory') }}) ,

base as (
    select
        ProductCategoryKey,
        ProductCategoryAlternateKey,
        EnglishProductCategoryName,
        SpanishProductCategoryName,
        FrenchProductCategoryName
    from categories
    where ProductCategoryKey is not null
)

select * from base