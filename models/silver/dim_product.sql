with 
products as ( select * from {{ ref('DimProduct') }}) ,

base as (

    select
        ProductKey,
        ProductAlternateKey,
        ProductSubcategoryKey,
        WeightUnitMeasureCode,
        SizeUnitMeasureCode,
        EnglishProductName,
        SpanishProductName,
        FrenchProductName,
        StandardCost,
        FinishedGoodsFlag,
        Color,
        SafetyStockLevel,
        ReorderPoint,
        ListPrice,
        Size,
        SizeRange,
        Weight,
        DaysToManufacture,
        ProductLine,
        DealerPrice,
        Class,
        Style,
        ModelName,
        EnglishDescription,
        FrenchDescription,
        ChineseDescription,
        ArabicDescription,
        HebrewDescription,
        ThaiDescription,
        GermanDescription,
        JapaneseDescription,
        TurkishDescription,
        StartDate,
        EndDate,
        Status
    from products
    where ProductKey is not null
)

select * from base
