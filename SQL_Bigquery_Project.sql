-- Big project for SQL
-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  SUM(totals.visits) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
GROUP BY 1
ORDER BY 1
-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT 
  trafficSource.source AS source,
  SUM(totals.visits) AS visits,
  SUM(totals.bounces) AS total_no_of_bounces,
  100*SUM(totals.bounces)/SUM(totals.visits) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
GROUP BY trafficSource.source
ORDER BY visits DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017
WITH time_detail AS (
SELECT 
  FORMAT_DATE('%Y%W', PARSE_DATE('%Y%m%d',date)) AS week,
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS mth,
  trafficSource.source AS source,
  totals.TransactionRevenue AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
)
SELECT
  'Month' AS time_type,
  mth AS time,
  source,
  SUM(revenue)
FROM time_detail 
GROUP BY source, mth
UNION ALL
SELECT
  'WEEK' AS time_type,
  week AS time,
  source,
  SUM(revenue)
FROM time_detail 
GROUP BY source, week
ORDER BY source, time
--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

WITH detail_mth AS (
  SELECT 
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,
  totals.transactions AS transactions,
  totals.pageviews AS pageview, 
  fullVisitorId
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE EXTRACT(month FROM PARSE_DATE('%Y%m%d', date)) = 6
  OR EXTRACT(month FROM PARSE_DATE('%Y%m%d', date)) = 7
)
SELECT
  month,
  SUM(CASE WHEN transactions >= 1 THEN 1 ELSE 0 END)
  /COUNT(DISTINCT CASE WHEN transactions >= 1 THEN fullVisitorId ELSE NULL END) 
  AS avg_pageviews_purchase,
  SUM(CASE WHEN transactions IS NULL THEN 1 ELSE 0 END)
  /COUNT(DISTINCT CASE WHEN transactions IS NULL THEN fullVisitorId ELSE NULL END) 
  AS avg_pageviews_non_purchase,
FROM detail_mth 
GROUP BY month
ORDER BY month


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
WITH detail_mth AS (SELECT 
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,
  totals.transactions AS transactions,
  fullVisitorId
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions >= 1
)
SELECT
  month,
  SUM(transactions)/ COUNT(DISTINCT fullVisitorId) AS Avg_total_transactions_per_user
FROM detail_mth 
GROUP BY month

-- Query 06: Average amount of money spent per session
#standardSQL
WITH detail_mth AS (SELECT 
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,
  totals.totalTransactionRevenue AS rev,
  fullVisitorId
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions IS NOT NULL
)
SELECT
  month,
  SUM(rev)/SUM(totals.visits) AS avg_revenue_by_user_per_visit
FROM detail_mth 
GROUP BY month


-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

select
    product.v2productname as other_purchased_product,
    sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
where fullvisitorid in (select distinct fullvisitorid
                        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                        unnest(hits) as hits,
                        unnest(hits.product) as product
                        where product.v2productname = "YouTube Men's Vintage Henley"
                        and hits.eCommerceAction.action_type = '6')
and product.v2productname != "YouTube Men's Vintage Henley"
and product.productRevenue is not null
group by other_purchased_product
order by quantity desc

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

WITH actions AS(
    SELECT 
      FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) AS month,
      eCommerceAction.action_type AS action
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST(hits)
    WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
)
SELECT 
  month,
  SUM(CASE WHEN action = '2' THEN 1 ELSE 0 END) AS num_product_view,
  SUM(CASE WHEN action = '3' THEN 1 ELSE 0 END) AS num_addtocart,
  SUM(CASE WHEN action = '6' THEN 1 ELSE 0 END) AS num_purchase,
  ROUND(100 * SUM(CASE WHEN action = '3' THEN 1 ELSE 0 END)
  /SUM(CASE WHEN action = '2' THEN 1 ELSE 0 END),2) AS add_to_cart_rate,
  ROUND(100 * SUM(CASE WHEN action = '6' THEN 1 ELSE 0 END) 
  /SUM(CASE WHEN action = '2' THEN 1 ELSE 0 END),2) AS purchase_rate
FROM actions
GROUP BY month 
ORDER BY month