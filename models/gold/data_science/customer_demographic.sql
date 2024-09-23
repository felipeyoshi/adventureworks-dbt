with
customers as ( select * from {{ ref('dim_customer') }}) ,
customer_segments as ( select * from {{ ref('dim_customer_segment') }}) ,
geo as ( select * from {{ ref('dim_geography') }}) ,

demographic_features as (
    select
        2014 - c.BirthYear as Age, -- Assuming the model ran in 2014
        c.MaritalStatus,
        c.Gender,
        c.YearlyIncome,
        c.TotalChildren,
        c.NumberChildrenAtHome,
        c.Education,
        c.Occupation,
        c.HouseOwnerFlag,
        c.NumberCarsOwned,
        g.EnglishCountryRegionName as Country,
        s.segment,
        s.bikebuyer
    from customers c
    left join geo g on c.GeographyKey = g.GeographyKey
    left join customer_segments s on c.customerkey = s.customerkey
)

select * from demographic_features