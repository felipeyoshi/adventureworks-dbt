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
        -- Recency Score: 1 to 5 scale (lower is better)
        percent_rank() over (order by recency) as recency_score,
        -- Frequency Score: 1 to 5 scale (higher is better)
        percent_rank() over (order by frequency) as frequency_score,
        -- Monetary Score: 1 to 5 scale (higher is better)
        percent_rank() over (order by monetary) as monetary_score
    from rfm_scores
),

customer_score as (
    select
        customerkey,   
        (0.2 * recency_score) + (0.4 * frequency_score) + (0.4 * monetary_score) as rfm_weights,
        percent_rank() over (order by (0.2 * recency_score) + (0.4 * frequency_score) + (0.4 * monetary_score)) as norm_rfm_score
   from rfm_segments
),

customer_segmentation as (
    select
        customerkey,
        norm_rfm_score,
        case
            when norm_rfm_score >= 0.90 then 'Best Customers'
            when norm_rfm_score >= 0.75 THEN 'Loyal Customers'
            when norm_rfm_score >= 0.50 THEN 'At Risk'
            else 'Other'
        end as segment
    from customer_score
    order by norm_rfm_score desc
)

select
    *
from customer_segmentation
