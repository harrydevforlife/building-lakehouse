
  
    
        create or replace table dev_silver.dim_date
      
      
    using delta
      
      
      
      
      
      as
      

select
  year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as dateInt,
  CalendarDate,
  year(calendarDate) AS CalendarYear,
  date_format(calendarDate, 'MMMM') as CalendarMonth,
  month(calendarDate) as MonthOfYear,
  date_format(calendarDate, 'EEEE') as CalendarDay,
  dayofweek(calendarDate) AS DayOfWeek,
  weekday(calendarDate) + 1 as DayOfWeekStartMonday,
  case
    when weekday(calendarDate) < 5 then 'Y'
    else 'N'
  end as IsWeekDay,
  dayofmonth(calendarDate) as DayOfMonth,
  case
    when calendarDate = last_day(calendarDate) then 'Y'
    else 'N'
  end as IsLastDayOfMonth,
  dayofyear(calendarDate) as DayOfYear,
  weekofyear(calendarDate) as WeekOfYearIso,
  quarter(calendarDate) as QuarterOfYear,
  /* Use fiscal periods needed by organization fiscal calendar */
  case
    when month(calendarDate) >= 10 then year(calendarDate) + 1
    else year(calendarDate)
  end as FiscalYearOctToSep,
  (month(calendarDate) + 2) % 12 + 1 AS FiscalMonthOctToSep,
  case
    when month(calendarDate) >= 7 then year(calendarDate) + 1
    else year(calendarDate)
  end as FiscalYearJulToJun,
  (month(calendarDate) + 5) % 12 + 1 AS FiscalMonthJulToJun
from
    (select explode(sequence(to_date('2000-01-01'), to_date('2050-12-31'), interval 1 day)) as calendarDate)
order by
  calendarDate
  