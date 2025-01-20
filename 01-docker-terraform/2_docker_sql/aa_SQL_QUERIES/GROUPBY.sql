SELECT 
	CAST(tpep_dropoff_datetime AS DATE) as "day",
	COUNT(1)
FROM 
	yellow_taxi_trips t 
GROUP BY 
	CAST(tpep_dropoff_datetime AS DATE)


SELECT 
	CAST(tpep_dropoff_datetime AS DATE) as "day",
	COUNT(1) AS "count"
FROM 
	yellow_taxi_trips t 
GROUP BY 
	CAST(tpep_dropoff_datetime AS DATE)
ORDER BY "count" DESC


SELECT 
	CAST(tpep_dropoff_datetime AS DATE) as "day",
	"DOLocationID",
	COUNT(1) AS "count",
	MAX(total_amount) AS max_total_amount,
	MAX(passenger_count) AS max_passenger_count
FROM 
	yellow_taxi_trips t 
GROUP BY 
	1, 2
ORDER BY "count" DESC