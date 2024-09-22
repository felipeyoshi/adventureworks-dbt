with 
internetsales as ( select * from {{ ref('FactInternetSales') }}) ,
resellersales as ( select * from {{ ref('FactResellerSales') }}) ,
currency as ( select * from {{ ref('FactCurrencyRate') }}) ,

sales_merged as (
    -- Join FactInternetSales with FactCurrencyRate
    select 
        fi.ProductKey,
        fi.OrderDateKey,
        fi.DueDateKey,
        fi.ShipDateKey,
        fi.CustomerKey,
        NULL as ResellerKey, -- FactInternetSales does not have ResellerKey
        NULL as EmployeeKey, -- FactInternetSales does not have EmployeeKey
        fi.PromotionKey,
        fi.CurrencyKey,
        fi.SalesTerritoryKey,
        fi.SalesOrderNumber,
        fi.SalesOrderLineNumber,
        fi.RevisionNumber,
        fi.OrderQuantity,
        fi.UnitPrice,
        fi.ExtendedAmount,
        fi.UnitPriceDiscountPct,
        fi.DiscountAmount,
        fi.ProductStandardCost,
        fi.TotalProductCost,
        fi.SalesAmount,
        fi.TaxAmt,
        fi.Freight,
        fi.CarrierTrackingNumber::string,
        fi.CustomerPONumber::string,
        fi.OrderDate,
        fi.DueDate,
        fi.ShipDate,
        -- Create new columns for currency-converted values
        round(fi.ExtendedAmount * cr.AverageRate, 2) as ExtendedAmountConverted,
        round(fi.DiscountAmount * cr.AverageRate, 2) as DiscountAmountConverted,
        round(fi.SalesAmount * cr.AverageRate, 2) as SalesAmountConverted,
        round(fi.ProductStandardCost * cr.AverageRate, 2) as ProductStandardCostConverted,
        round(fi.TotalProductCost * cr.AverageRate, 2) as TotalProductCostConverted,
        round(fi.TaxAmt * cr.AverageRate, 2) as TaxAmtConverted,
        round(fi.Freight * cr.AverageRate, 2) as FreightConverted,
        'Internet' as SalesChannel
    from internetsales fi
    join currency cr
        on fi.CurrencyKey = cr.CurrencyKey
        and fi.OrderDateKey = cr.DateKey
    
    union all
    
    -- Repeat for FactResellerSales
    select 
        fr.ProductKey,
        fr.OrderDateKey,
        fr.DueDateKey,
        fr.ShipDateKey,
        NULL as CustomerKey, -- FactResellerSales does not have CustomerKey
        fr.ResellerKey,
        fr.EmployeeKey,
        fr.PromotionKey,
        fr.CurrencyKey,
        fr.SalesTerritoryKey,
        fr.SalesOrderNumber,
        fr.SalesOrderLineNumber,
        fr.RevisionNumber,
        fr.OrderQuantity,
        fr.UnitPrice,
        fr.ExtendedAmount,
        fr.UnitPriceDiscountPct,
        fr.DiscountAmount,
        fr.ProductStandardCost,
        fr.TotalProductCost,
        fr.SalesAmount,
        fr.TaxAmt,
        fr.Freight,
        fr.CarrierTrackingNumber::string,
        fr.CustomerPONumber::string,
        fr.OrderDate,
        fr.DueDate,
        fr.ShipDate,
        -- Create new columns for currency-converted values
        round(fr.ExtendedAmount * cr.AverageRate, 2) as ExtendedAmountConverted,
        round(fr.DiscountAmount * cr.AverageRate, 2) as DiscountAmountConverted,
        round(fr.SalesAmount * cr.AverageRate, 2) as SalesAmountConverted,
        round(fr.ProductStandardCost * cr.AverageRate, 2) as ProductStandardCostConverted,
        round(fr.TotalProductCost * cr.AverageRate, 2) as TotalProductCostConverted,
        round(fr.TaxAmt * cr.AverageRate, 2) as TaxAmtConverted,
        round(fr.Freight * cr.AverageRate, 2) as FreightConverted,
        'Reseller' as SalesChannel
    from resellersales fr
    join currency cr
        on fr.CurrencyKey = cr.CurrencyKey
        and fr.OrderDateKey = cr.DateKey
)

-- Final select to ensure we pick all the relevant columns, including the newly converted fields
select * from sales_merged