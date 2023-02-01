-- Count trips on Jan 15, 2019
SELECT
	COUNT(1)
FROM green_taxi_trips
WHERE to_char(lpep_pickup_datetime, 'YYYY-MM-DD') = '2019-01-15'AND 
	to_char(lpep_dropoff_datetime, 'YYYY-MM-DD') = '2019-01-15'

-- another way to check for dates
SELECT
	COUNT(1)
FROM green_taxi_trips
WHERE date(lpep_pickup_datetime) = '2019-01-15'AND 
	date(lpep_dropoff_datetime) = '2019-01-15'

-- ans: 20530


-- largest trip distance
--- date with largest trip distance
SELECT 
	MAX(Trip_distance) as dist, date(lpep_pickup_datetime) as pickupdate
FROM green_taxi_trips
GROUP BY pickupdate
ORDER BY dist DESC
-- ans: "2019-01-15" 117.99


--how many trips with 2, 3 passengers
SELECT 
	COUNT(1) as two
FROM green_taxi_trips
WHERE Passenger_count = 2 AND to_char(lpep_pickup_datetime, 'YYYY-MM-DD') = '2019-01-01'
-- 2 - 1282 3 - 254


-- Astoria zone, largest trip
SELECT MAX(tip_amount) as tip, green_taxi_trips."DOLocationID"
FROM green_taxi_trips
JOIN zones ON green_taxi_trips."PULocationID"=zones."LocationID"
WHERE zones."Zone" = 'Astoria'
GROUP BY green_taxi_trips."DOLocationID"
ORDER BY tip DESC

SELECT zones."Zone"
FROM green_taxi_trips
JOIN zones ON green_taxi_trips."DOLocationID"=zones."LocationID"
WHERE green_taxi_trips."DOLocationID" = 146

--ONE QUERY
SELECT foo.tip, foo."DOLocationID", zones."Zone"
FROM
	(SELECT MAX(tip_amount) as tip, green_taxi_trips."DOLocationID"
	FROM green_taxi_trips
	JOIN zones ON green_taxi_trips."PULocationID"=zones."LocationID"
	WHERE zones."Zone" = 'Astoria'
	GROUP BY green_taxi_trips."DOLocationID") as foo
		JOIN zones ON foo."DOLocationID"=zones."LocationID"
ORDER BY foo.tip DESC
-- 88, DO 146, Long Island City/Queens Plaza


-- out of curiosity, not part of homework
--- date with maximum time spent
SELECT pickupdate, time_spent
FROM
	(
		SELECT date(lpep_pickup_datetime) as pickupdate, 
			   (lpep_dropoff_datetime - lpep_pickup_datetime) as time_spent
		FROM green_taxi_trips
	) as bob
WHERE time_spent = (SELECT MAX(time_spent) FROM 
	(SELECT
		date(lpep_pickup_datetime) as pickupdate,
		MAX(lpep_dropoff_datetime - lpep_pickup_datetime) as time_spent
	FROM
		green_taxi_trips
	GROUP BY pickupdate) AS foo)
-- "23:59:58" "2019-01-25"