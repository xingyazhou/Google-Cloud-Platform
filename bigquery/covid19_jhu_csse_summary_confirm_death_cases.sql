/* 
 * total confirmed and newly added covid19 cases per conutry per day
 * total deaths and newly added death cases per country perday 
 */

WITH
  summary AS(
    SELECT
      country_region,
      date,
      SUM( confirmed) AS confirmed,
      SUM(deaths) AS deaths,
    FROM `bigquery-public-data.covid19_jhu_csse.summary`
    GROUP BY country_region, date),
  
  input AS(
    SELECT
      country_region,
      date,
      confirmed ,
      /* 
       * LAG() function: 
       * First, the PARTITION BY clause divided the result set into groups by country_region.
       * Second, for each group, the ORDER BY clause sorted the rows by date in ascending order.
       * Third, LAG() function applied to the row of each group independently. The first row in each group was NULL because there was no previous confirmed. 
       * The second and third row gots the new cases from the first and second row and populated them into the preceding_confirmed column.
      */
      LAG(confirmed) OVER (PARTITION BY country_region order by date asc) as preceding_confirmed,
      deaths,
      LAG(deaths) OVER (PARTITION BY country_region order by date asc) as preceding_deaths
    FROM summary  
  )
  
select
  country_region,
  date,
  confirmed as total_confirmed,
  if (confirmed - preceding_confirmed<0, 0, confirmed - preceding_confirmed) as new_cases,
  deaths as total_deaths,
  preceding_deaths,
  deaths -  preceding_deaths as new_deaths
from input

