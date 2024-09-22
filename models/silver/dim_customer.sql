with 
customers as ( select * from {{ ref('DimCustomer') }}) ,

base as (

    select
        CustomerKey,
        GeographyKey,
        CustomerAlternateKey,
        Title,
        FirstName,
        MiddleName,
        LastName,
        NameStyle,
        BirthDate,
        MaritalStatus,
        Suffix,
        Gender,
        EmailAddress,
        YearlyIncome,
        TotalChildren,
        NumberChildrenAtHome,
        coalesce(EnglishEducation, SpanishEducation, FrenchEducation) as Education,
        coalesce(EnglishOccupation, SpanishOccupation, FrenchOccupation) as Occupation,
        HouseOwnerFlag,
        NumberCarsOwned,
        AddressLine1,
        AddressLine2,
        Phone,
        DateFirstPurchase,
        CommuteDistance
    from customers
    where CustomerKey is not null

),

-- Handle any necessary data quality checks, transformations, or deduplication
cleaned as (
    select
        *,
        case 
            when MaritalStatus = 'M' then 'Married'
            when MaritalStatus = 'S' then 'Single'
            else 'Unknown'
        end as MaritalStatusFormatted,
        case 
            when Gender = 'M' then 'Male'
            when Gender = 'F' then 'Female'
            else 'Unknown'
        end as GenderFormatted,
        year(BirthDate) as BirthYear
    from base
)

select * from cleaned
