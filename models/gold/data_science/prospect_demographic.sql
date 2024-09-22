with
prospects as ( select * from {{ ref('fact_prospective_buyer') }}) ,

demographic_features as (
    select
        ProspectiveBuyerKey,
        2014 - year(BirthDate) as Age,
        MaritalStatus,
        Gender,
        YearlyIncome,
        TotalChildren,
        NumberChildrenAtHome,
        Education,
        Occupation,
        HouseOwnerFlag,
        NumberCarsOwned,
        City,
        StateProvinceCode
    from prospects
)

select * from demographic_features