--bai1
WITH CTE1 AS
(SELECT 
EXTRACT(year from transaction_date) AS year, 
product_id, 
sum(spend) AS curr_year_spend	
FROM user_transactions
group by EXTRACT(year from transaction_date), product_id)

SELECT 
year, 
product_id,
curr_year_spend,
LAG(curr_year_spend) OVER(PARTITION BY product_id ORDER BY year) as prev_year_spend,
round(
(curr_year_spend - LAG(curr_year_spend) OVER(PARTITION BY product_id ORDER BY year))
/ (LAG(curr_year_spend) OVER(PARTITION BY product_id ORDER BY year)) *100, 
2)	as yoy_rate
FROM CTE1

--bai2
  SELECT DISTINCT card_name, 
FIRST_VALUE (issued_amount) OVER(PARTITION BY card_name ORDER BY issued_amount) AS issued_amount
FROM monthly_cards_issued
group by card_name, issued_amount
ORDER BY issued_amount DESC

--bai3
  WITH CTE1 AS
(SELECT user_id, 
spend,
transaction_date,
ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY transaction_date) AS rank3
FROM transactions)

SELECT 
user_id,
spend,
transaction_date
FROM CTE1
WHERE rank3 ='3'

--bai4
WITH CTE1 AS
(SELECT 
transaction_date,
user_id,
COUNT(product_id) AS purchase_count,
RANK() OVER(PARTITION BY user_id ORDER BY transaction_date DESC) AS transaction_date1
FROM user_transactions
GROUP BY user_id, transaction_date)

SELECT
transaction_date,
user_id,
purchase_count
FROM CTE1
where transaction_date1 = 1
GROUP BY user_id, transaction_date, purchase_count
ORDER BY transaction_date

--bai5
SELECT 
user_id, 
tweet_date, 	
ROUND(AVG(tweet_count) OVER(PARTITION BY user_id 
ORDER BY tweet_date
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),2)
as AVG_rolling_avg_3d
FROM tweets;

--Bai 6
SELECT
transaction_date,
user_id,
purchase_count
FROM CTE1
where transaction_date1 = 1
GROUP BY user_id, transaction_date, purchase_countSELECT 
user_id, 
tweet_date, 	
ROUND(AVG(tweet_count) OVER(PARTITION BY user_id 
ORDER BY tweet_date
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),2)  --cainaymoi
as AVG_rolling_avg_3d
FROM tweets;

--bai 7
-- identify the two top highest-grossing product within each category in 2022
WITH CTE1 AS
(SELECT 
category, 
product,
sum(spend) OVER(PARTITION BY product) as total_spend
from product_spend
where extract(year from transaction_date)= 2022),

CTE2 AS
(SELECT 
category, 
product,
total_spend,
RANK() OVER(PARTITION BY category ORDER BY total_spend DESC) AS rank1
FROM CTE1
GROUP by category, product, total_spend)

SELECT 
category, 
product,
total_spend
FROM CTE2
WHERE rank1 <=2

--bai8: top 5 artist whose songs appear most frequently in top 10
WITH CTE1 AS
(SELECT 
a.artist_name, 
DENSE_RANK() OVER (ORDER BY COUNT(b.song_id) DESC) AS artist_rank
from artists as a    
JOIN songs AS b on a.artist_id = b.artist_id	
JOIN global_song_rank as c on b.song_id=c.song_id
WHERE c.rank between 1 and 10
group by a.artist_name)

SELECT 
artist_name, 
artist_rank
FROM CTE1
WHERE artist_rank <=5
