with 
dates as ( select * from {{ ref('DimDate') }}) ,

base as (
    select
        DateKey,
        FullDateAlternateKey,
        DayNumberOfWeek,
        EnglishDayNameOfWeek,
        SpanishDayNameOfWeek,
        FrenchDayNameOfWeek,
        DayNumberOfMonth,
        DayNumberOfYear,
        WeekNumberOfYear,
        EnglishMonthName,
        SpanishMonthName,
        FrenchMonthName,
        MonthNumberOfYear,
        CalendarQuarter,
        CalendarYear,
        CalendarSemester,
        FiscalQuarter,
        FiscalYear,
        FiscalSemester
    from dates
    where DateKey is not null
)

select * from base