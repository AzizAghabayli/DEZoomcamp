### Queries used to answer questions for Worshop 2

### Question 1:

_Create a materialized view to compute the average, min and max trip time between each taxi zone._

```sql
CREATE MATERIALIZED VIEW trip_times AS
	SELECT
		t1.Zone AS pickup_zone,
		t2.Zone AS dropoff_zone,
		MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time,
		MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
		AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time
	FROM trip_data
	JOIN taxi_zone AS t1 ON trip_data.PULocationID = t1.location_id
	JOIN taxi_zone AS t2 ON trip_data.DOLocationID = t2.location_id
	GROUP BY t1.Zone, t2.Zone;
```

##### Output:

```
dev=> CREATE MATERIALIZED VIEW trip_times AS
dev->   SELECT
dev->           t1.Zone AS pickup_zone,
dev->           t2.Zone AS dropoff_zone,
dev->           MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time,
dev->           MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
dev->           AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time
dev->   FROM trip_data
dev->   JOIN taxi_zone AS t1 ON trip_data.PULocationID = t1.location_id
dev->   JOIN taxi_zone AS t2 ON trip_data.DOLocationID = t2.location_id
dev->   GROUP BY t1.Zone, t2.Zone;
CREATE_MATERIALIZED_VIEW

dev=> SELECT * FROM trip_times LIMIT 10;
  pickup_zone  |          dropoff_zone          | max_trip_time | min_trip_time |  avg_trip_time
---------------+--------------------------------+---------------+---------------+-----------------
 Alphabet City | East Chelsea                   | 00:22:31      | 00:12:21      | 00:17:00
 Alphabet City | Flatiron                       | 00:11:48      | 00:07:38      | 00:09:43
 Alphabet City | Greenwich Village South        | 00:06:36      | 00:06:36      | 00:06:36
 Alphabet City | Lower East Side                | 00:10:47      | 00:03:12      | 00:05:58.4
 Alphabet City | Two Bridges/Seward Park        | 23:57:21      | 00:05:37      | 08:03:11.333333
 Astoria       | Midtown Center                 | 00:17:32      | 00:17:32      | 00:17:32
 Astoria       | Midtown South                  | 00:12:22      | 00:12:22      | 00:12:22
 Astoria       | Park Slope                     | 00:23:07      | 00:23:07      | 00:23:07
 Astoria       | Queensbridge/Ravenswood        | 00:10:30      | 00:01:22      | 00:03:55
 Astoria       | Stuy Town/Peter Cooper Village | 00:15:44      | 00:15:44      | 00:15:44
(10 rows)
```

_Create a materialized view to compute the average, min and max trip time between each taxi zone._

```sql
WITH max_avg_trip_time AS (SELECT MAX(avg_trip_time) max_avg FROM trip_times)
SELECT pickup_zone, dropoff_zone, avg_trip_time FROM trip_times, max_avg_trip_time
WHERE avg_trip_time = max_avg;
```

##### Output:

```
  pickup_zone   | dropoff_zone | avg_trip_time
----------------+--------------+---------------
 Yorkville East | Steinway     | 23:59:33
(1 row)
```

### Question 2:

_Find the number of trips for the pair of taxi zones with the highest average trip time._

```sql
CREATE MATERIALIZED VIEW trips_count AS
	SELECT
		t1.Zone AS pickup_zone,
		t2.Zone AS dropoff_zone,
		AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
		COUNT(*) AS number_of_trips
	FROM trip_data
	JOIN taxi_zone AS t1 ON trip_data.PULocationID = t1.location_id
	JOIN taxi_zone AS t2 ON trip_data.DOLocationID = t2.location_id
	GROUP BY t1.Zone, t2.Zone;
```

```sql
WITH max_avg_trip_time AS (SELECT MAX(avg_trip_time) max_avg FROM trips_count)
SELECT pickup_zone, dropoff_zone, number_of_trips FROM trips_count, max_avg_trip_time
WHERE trips_count.avg_trip_time = max_avg_trip_time.max_avg;
```

##### Output:

```
  pickup_zone   | dropoff_zone | number_of_trips
----------------+--------------+-----------------
 Yorkville East | Steinway     |               1
(1 row)
```

### Question 3:

_What are the top 3 busiest zones in terms of number of pickups?_

```sql

```
