with 
internet_sales_reason as ( select * from {{ ref('FactInternetSalesReason') }}) ,

base as (
    select
        SalesOrderNumber,
        SalesOrderLineNumber,
        SalesReasonKey
    from internet_sales_reason
    where SalesOrderNumber is not null
      and SalesOrderLineNumber is not null
      and SalesReasonKey is not null
)

select * from base