SELECT 
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	"PULocationID",
	"DOLocationID"
FROM yellow_taxi_trips t
WHERE "PULocationID" NOT IN (
	SELECT "LocationID"
	FROM zones
)
LIMIT 100