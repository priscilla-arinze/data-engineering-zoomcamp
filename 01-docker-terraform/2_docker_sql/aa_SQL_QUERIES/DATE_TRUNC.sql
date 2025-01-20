SELECT 
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	DATE_TRUNC('DAY', tpep_dropoff_datetime),
	total_amount
FROM 
	yellow_taxi_trips t 
LIMIT 100