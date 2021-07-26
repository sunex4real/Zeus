/* How many sessions are there? */
SELECT
  COUNT(DISTINCT CONCAT(fullvisitorid, CAST(visitid AS string))) AS sessions
FROM
  `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`



/* How many sessions does each visitor create? */
SELECT
  COUNT(DISTINCT CONCAT(fullvisitorid, CAST(visitid AS string))) / COUNT(DISTINCT fullvisitorid) AS sessions_per_user
FROM
  `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`


/* How much time does it take on average to reach the order_confirmation screen per
session (in minutes)? */

WITH
  transactions AS (
  SELECT
    fullvisitorid,
    visitNumber,
    visitId,
    visitStartTime,
    date,
    eventCategory,
    eventAction,
    screenName,
    landingScreenName,
    time,
    ROW_NUMBER() OVER(PARTITION BY fullvisitorid, visitId, date, visitStartTime ORDER BY time ASC) AS rank
  FROM
    `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` ga,
    UNNEST(ga.hit)
  WHERE
    eventCategory LIKE '%order_confirmation%')
SELECT
  ROUND(AVG(time / 60000)) as avg_time_mins
FROM
  transactions
WHERE
  rank = 1