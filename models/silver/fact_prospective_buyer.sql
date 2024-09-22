with 
prospective_buyers as ( select * from {{ ref('FactProspectiveBuyer') }}) ,

base as (
    select
        ProspectiveBuyerKey,
        ProspectAlternateKey,
        FirstName,
        MiddleName,
        LastName,
        BirthDate,
        MaritalStatus,
        Gender,
        EmailAddress,
        YearlyIncome,
        TotalChildren,
        NumberChildrenAtHome,
        Education,
        Occupation,
        HouseOwnerFlag,
        NumberCarsOwned,
        AddressLine1,
        AddressLine2,
        City,
        StateProvinceCode,
        PostalCode,
        Phone,
        Salutation,
        Unknown
    from prospective_buyers
    where ProspectiveBuyerKey is not null
)

select * from base