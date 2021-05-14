import json
import configparser

from influx_database import InfluxDatabase


config = configparser.ConfigParser()
config.read("config.ini")
config_db = config['influxdb']

influx_db = InfluxDatabase(
    token=config_db['Token'], 
    org=config_db['Organization'], 
    bucket=config_db['Bucket'], 
    ipaddress=config_db['IpAddress'], 
    tag_name=config_db['TagName'],
    sensor_tag_name=config_db['SensorTagName'],
    output_directory="./output")


if __name__ == "__main__":

    # Load name mapper if sensor nodes have different initial/naming
    with open('./name_mapper.json', 'r') as file:
        name_mapper = json.load(file)
        file.close()
    # Load query job, timestamp in universal time
    with open('./query_job.json', 'r') as file:
        query_job = json.load(file)
        file.close()

    for date, marking in query_job.items():

        influx_db.export_data_as_csv(
            time_start=f"{date}T{marking['start']}Z", 
            time_stop=f"{date}T{marking['stop']}Z", 
            name_mapper=name_mapper)
