-- This model calculates the RFM (Recency, Frequency, Monetary) scores for customer segmentation,
-- only selecting the CUSTOMERKEY and segment fields.
with base_customers as (
    select
        c.customerkey,
        max(orderdate) as LastOrderDate,
        count(distinct salesordernumber) as TotalOrders,
        sum(SalesAmountConverted) as TotalSpent
    from {{ ref('dim_customer') }} as c
    left join {{ ref('fact_sales') }} as s
    on c.customerkey = s.customerkey
    group by 
        c.customerkey
),

rfm_scores as (
    -- Calculate Recency (R), Frequency (F), and Monetary (M) scores
    select
        customerkey,
        -- Recency: how recently the customer made a purchase (days since last purchase)
        datediff('day', LastOrderDate, '2014-01-01') as recency,
        -- Frequency: how many purchases the customer made
        TotalOrders as frequency,
        -- Monetary: total spending by the customer
        TotalSpent as monetary
    from base_customers
),

rfm_segments as (
    -- Apply RFM segmentation to assign a score between 1 and 5 for each R, F, and M value
    select
        customerkey,
        -- Recency Score: 0 to 1 scale (lower is better)
        percent_rank() over (order by recency) as recency_score,
        -- Frequency Score: 0 to 1 scale (higher is better)
        percent_rank() over (order by frequency desc) as frequency_score,
        -- Monetary Score: 0 to 1 scale (higher is better)
        percent_rank() over (order by monetary desc) as monetary_score
    from rfm_scores
),

customer_score as (
    select
        customerkey,
        recency_score,
        frequency_score,
        monetary_score,
        (0.2 * recency_score) + (0.4 * frequency_score) + (0.4 * monetary_score) as rfm_weights,
        percent_rank() over (order by (0.2 * recency_score) + (0.4 * frequency_score) + (0.4 * monetary_score)) as norm_rfm_score
   from rfm_segments
),

customer_segmentation as (
    select
        *,
        case
            when norm_rfm_score >= 0.90 then 'Best Customers'
            when norm_rfm_score >= 0.75 THEN 'Loyal Customers'
            when norm_rfm_score >= 0.50 THEN 'At Risk'
            else 'Other'
        end as segment
    from customer_score
    order by norm_rfm_score desc
),

-- Identify bike purchases
bike_purchases as (
    select
        s.customerkey,
        sum(case when pc.EnglishProductCategoryName = 'Bikes' then s.orderquantity else 0 end) as NumberBikesOwned,
        case when sum(case when pc.EnglishProductCategoryName = 'Bikes' then 1 else 0 end) > 0 then 1 else 0 end as BikeBuyer
    from {{ ref('fact_sales') }} as s
    left join {{ ref('dim_product') }} as p
    on s.ProductKey = p.ProductKey
    left join {{ ref('dim_product_subcategory') }} as ps
    on p.ProductSubCategoryKey = ps.ProductSubCategoryKey
    left join {{ ref('dim_product_category') }} as pc
    on ps.ProductCategoryKey = pc.ProductCategoryKey
    group by s.customerkey
)

select
    s.*,
    p.BikeBuyer,
    p.NumberBikesOwned
from customer_segmentation s
left join bike_purchases p
on s.customerkey = p.customerkey