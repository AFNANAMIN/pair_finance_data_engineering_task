### The amount of data points aggregated for every device per hours. ###
<img width="989" alt="image" src="https://user-images.githubusercontent.com/33934146/188298665-47f1b900-46f6-41f5-8a35-6108a2cafa57.jpeg">


### The maximum temperatures measured for every device per hours. ###
<img width="963" alt="image" src="https://user-images.githubusercontent.com/33934146/188298766-3b55bfd7-5b6c-4a9d-9c56-f045c0e045c3.jpeg">

### Total distance of device movement for every device per hours. ###

<img width="1189" alt="image" src="https://user-images.githubusercontent.com/33934146/188298785-ad9d0e58-4428-4ebb-bd97-6b428dd60762.jpeg">






## Running the docker

To get started run ``` docker-compose up ``` in root directory.
It will create the PostgresSQL database and start generating the data.
It will create an empty MySQL database.
It will launch the analytics.py script. 

Your task will be to write the ETL script inside the analytics/analytics.py file.