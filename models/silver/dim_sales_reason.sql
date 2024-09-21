with
sales_reasons as ( select * from {{ ref('DimSalesReason') }}) ,

base as (
    select
        SalesReasonKey,
        SalesReasonAlternateKey,
        SalesReasonName,
        SalesReasonReasonType
    from sales_reasons
    where SalesReasonKey is not null
)

select * from base
