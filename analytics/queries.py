
def max_temp(pg_engine,mysql_engine):
    filter_columns_query = '''SELECT d.device_id,extract(hour from (to_timestamp(d.time::numeric))) as hour, MAX(d.temperature) as temperature
FROM devices d GROUP BY hour,device_id '''
    result=pg_engine.execute(filter_columns_query)
    for row in result.fetchall():
        mysql_engine.execute("INSERT INTO max_temp (device_id,hour,temperature) VALUES (%s, %s, %s)", \
                             (row['device_id'], row['hour'], row['temperature']))
    print(result)

def count_data(pg_engine,mysql_engine):
    filter_columns_query = '''SELECT d.device_id,extract(hour from (to_timestamp(d.time::numeric))) as hour, count(1) as data_count
FROM devices d GROUP BY hour,device_id ;  '''
    result=pg_engine.execute(filter_columns_query)
    for row in result.fetchall():
        mysql_engine.execute("INSERT INTO count_data (device_id,hour,data_count) VALUES (%s, %s, %s)", \
                        (row['device_id'], row['hour'], row['data_count']))


    print(result)

def total_distance(pg_engine,mysql_engine):
    filter_columns_query = '''with distance as ( SELECT cast(a.location::json->>'longitude' as decimal) as Longitude_a,
cast(a.location::json->>'latitude' as decimal) as Latitude_a,
cast(b.location::json->>'longitude' as decimal) as Longitude_b,
cast(b.location::json->>'latitude' as decimal) as Latitude_b,
a.device_id as device_id ,
extract(hour from (to_timestamp(a.time::numeric))) as hour
FROM devices AS a
JOIN devices AS b ON a.device_id = b.device_id limit 10 )

select  device_id,hour,111.111 *
    DEGREES(ACOS(LEAST(1.0, COS(RADIANS(Latitude_a))
         * COS(RADIANS(Latitude_b))
         * COS(RADIANS(Longitude_a - Longitude_b))
         + SIN(RADIANS(Latitude_a))
         * SIN(RADIANS(Latitude_b))))) AS distance_in_km
         
         from distance group by hour,device_id,distance_in_km ;

 '''
    result=pg_engine.execute(filter_columns_query)

    for row in result.fetchall():
        mysql_engine.execute("INSERT INTO total_distance (device_id,hour,distance_in_km) VALUES (%s, %s, %s)", \
                             (row['device_id'], row['hour'], row['distance_in_km']))

    print(result)

