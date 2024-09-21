with 
promotions as ( select * from {{ ref('DimPromotion') }}) ,

base as (
    select
        PromotionKey,
        PromotionAlternateKey,
        EnglishPromotionName,
        SpanishPromotionName,
        FrenchPromotionName,
        DiscountPct,
        EnglishPromotionType,
        SpanishPromotionType,
        FrenchPromotionType,
        EnglishPromotionCategory,
        SpanishPromotionCategory,
        FrenchPromotionCategory,
        StartDate,
        EndDate,
        MinQty,
        MaxQty
    from promotions
    where PromotionKey is not null
)

select * from base