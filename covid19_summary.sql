
/*  Data Source:  `bigquery-public-data.covid19_jhu_csse.summary`
 *
 *  This Query is designed to answer following questions: 
 *  Total Coronavirus Cases per conutry per day ?
 *  Daily New Coronavirus cases per country per day ?
 *  Total Coronavirus Deaths per conutry per day ?
 *  Daily New Coronavirus Deaths per country per day ?
 *
 */

WITH
  summary AS
  (
    SELECT
      country_region,
      date,
      SUM(confirmed) AS confirmed,
      SUM(deaths) AS deaths,
    FROM `bigquery-public-data.covid19_jhu_csse.summary`
    GROUP BY country_region, date
  )

  intermediate AS
  (
    SELECT
      country_region,
      date,
      confirmed ,

      /*  LAG() function:  
       *  First, the PARTITION BY clause divided the result set into groups by country_region
       *  Second, for each group, the ORDER BY clause sorted the rows by date in ascending order
       *  Third, LAG() function applied to the row of each group independently. The first row in each group was NULL because there was no previous confirmed
       *  The second and third row gets the new cases from the first and second row and populated them into the preceding_confirmed column
       */
      LAG(confirmed) OVER (PARTITION BY country_region ORDER BY date ASC) AS preceding_confirmed

      deaths
      LAG(deaths) OVER (PARTITION BY country_region ORDER BY date ASC) AS preceding_deaths
    FROM summary
  )

SELECT
  country_region,
  date,
  confirmed as total_confirmed,
  IF (confirmed - preceding_confirmed < 0, 0, confirmed - preceding_confirmed) AS new_confirmed,
  deaths as total_deaths,
  IF (deaths - preceding_deaths < 0, 0, deaths - preceding_deaths) AS new_deaths
FROM intermediate