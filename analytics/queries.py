
def max_temp(pg_engine,mysql_engine):
    filter_columns_query = '''SELECT d.device_id,extract(hour from (to_timestamp(d.time::numeric))) as hour, MAX(d.temperature) as temperature
FROM devices d GROUP BY hour,device_id '''
    #storing data in mysql db
    result=pg_engine.execute(filter_columns_query)
    print(result)
    mysql_engine.execute("CREATE TABLE IF NOT EXISTS max_temp (device_id VARCHAR(255), hour integer, temperature integer)")
    for row in result.fetchall():
        mysql_engine.execute("INSERT INTO max_temp (device_id,hour,temperature) VALUES (%s, %s, %s)", \
                             (row['device_id'], row['hour'], row['temperature']))


def count_data(pg_engine,mysql_engine):
    filter_columns_query = '''SELECT d.device_id,extract(hour from (to_timestamp(d.time::numeric))) as hour, count(1) as data_count
FROM devices d GROUP BY hour,device_id ;  '''
    #storing data in mysql db
    result=pg_engine.execute(filter_columns_query)
    mysql_engine.execute("CREATE TABLE IF NOT EXISTS count_data (device_id VARCHAR(255), hour integer, data_count integer)")
    for row in result.fetchall():
        mysql_engine.execute("INSERT INTO count_data (device_id,hour,data_count) VALUES (%s, %s, %s)", \
                        (row['device_id'], row['hour'], row['data_count']))



def total_distance(pg_engine,mysql_engine):
    filter_columns_query = '''
    with data_with_starting_ending_times AS (
    SELECT
        d.device_id, EXTRACT(hour from (TO_TIMESTAMP(d.time::numeric))) AS hour, MIN(d.time) AS starting_time, MAX(d.time) AS ending_time
    FROM devices d GROUP BY d.device_id,hour
),

data_with_starting_location AS (
    SELECT
        d.location, d.device_id, ag.hour
    FROM devices d
        JOIN data_with_starting_ending_times ag on d.device_id=ag.device_id AND d.time=ag.starting_time
),

data_with_ending_location AS (
    SELECT
        d.location, d.device_id, ag.hour
    FROM devices d
        JOIN data_with_starting_ending_times ag on d.device_id=ag.device_id AND d.time=ag.ending_time
),

aggregated_data AS (
    SELECT
        s.device_id,
        s.hour,
        cast(s.location::json->>'longitude' as decimal) as starting_longitude,
        cast(s.location::json->>'latitude' as decimal) as starting_latitude,
        cast(e.location::json->>'longitude' as decimal) as ending_longitude,
        cast(e.location::json->>'latitude' as decimal) as ending_latitude
    FROM data_with_starting_location s JOIN data_with_ending_location e ON s.device_id=e.device_id AND s.hour=e.hour
)

SELECT
    device_id,
    hour,
    111.111 *
    DEGREES(ACOS(LEAST(1.0, COS(RADIANS(starting_latitude))
         * COS(RADIANS(ending_latitude))
         * COS(RADIANS(starting_longitude - ending_longitude))
         + SIN(RADIANS(starting_latitude))
         * SIN(RADIANS(ending_latitude))))) AS distance_in_km
FROM aggregated_data;

 '''
    result=pg_engine.execute(filter_columns_query)
    print(result)
    #storing data in mysql db
    mysql_engine.execute("CREATE TABLE IF NOT EXISTS total_distance (device_id VARCHAR(255), hour integer, distance_in_km integer)")

    for row in result.fetchall():
        mysql_engine.execute("INSERT INTO total_distance (device_id,hour,distance_in_km) VALUES (%s, %s, %s)", \
                             (row['device_id'], row['hour'], row['distance_in_km']))



