SELECT 
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pick_up_loc",
	CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc"
FROM 
	yellow_taxi_trips t 
        JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
	    JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
LIMIT 10