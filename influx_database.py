import os
import csv

import pandas as pd
from influxdb_client import InfluxDBClient, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS


class Directory:

    def __init__(self, output_directory:str):
        self.directory = output_directory
        self._initialize_base_directory()

    def _get_folder_name(self, path:str) -> str:
        
        omitted_chars = "/?!:,._-*"

        for char in omitted_chars:
            path = path.replace(char, '')            

        return path

    def _initialize_base_directory(self):
        
        if not os.path.exists(f"{self.directory}"):
            os.mkdir(f"{self.directory}")

    def create_folder(self, folder_name:str) -> None:

        name = self._get_folder_name(folder_name)

        if not os.path.exists(f"{self.directory}/{name}"):
            os.mkdir(f"{self.directory}/{name}")


class InfluxDatabaseQuery(Directory):

    def __init__(self, client:InfluxDBClient, org:str, bucket:str, output_directory:str):
        self.__client = client
        self.__org = org
        self.__bucket = bucket
        self.__output_directory = output_directory
        self.__query_api = self.__client.query_api()
        self.__measurements = self.list_measurements()
        self.__fields = self.list_fields_in_measurements()
        self.__tags = self.list_unique_tag_values()
        self.__data_structure = {
            "": 0,
            "_result": 1,
            "table": 2,
            "_start": 3,
            "_stop": 4,
            "_time": 5,
            "_value": 6,
            "_field": 7,
            "_measurement": 8,  # default data structure up to here
            "device_serial_number": 9, # tag name starts here
            "log_unit": 10,
            "parameter_name": 11 # 
        }
        super().__init__(output_directory=output_directory)

    def measurement_names(self):
            return self.__measurements

    def query_as_table(self, query:str) -> list:

        result = []

        tables = self.__query_api.query(
                            org=self.__org,
                            query=query)

        for table in tables:
            for row in table:
                result.append(row["_value"])
                
        return result

    def query_as_csv(self, query:str) -> csv.reader:

        return self.__query_api.query_csv(
                org=self.__org,
                query=query, 
                dialect=Dialect(
                header=False, 
                delimiter=",", 
                comment_prefix="#", 
                annotations=[],
                date_time_format="RFC3339"))

    def list_measurements(self) -> list:

        query = f'''
            import "influxdata/influxdb/schema" 
            schema.measurements(bucket: "{self.__bucket}")
        '''
        return self.query_as_table(query)

    def list_fields_in_measurements(self) -> dict:

        fields = {}

        for measurement in self.__measurements:
            query = f'''
                import "influxdata/influxdb/schema"

                schema.measurementTagValues(
                bucket: "{self.__bucket}",
                measurement: "{measurement}",
                tag: "_field"
                )
            '''   
            fields[measurement] = self.query_as_table(query)

        print(fields)            
        return fields

    def list_unique_tag_values(self) -> dict:

        unique_tags = {}

        tags = ["device_serial_number", "parameter_name"]

        for tag in tags:
            query = f'''
                import "influxdata/influxdb/schema"

                schema.tagValues(bucket: "{self.__bucket}", tag: "{tag}")
            '''
            unique_tags[tag] = self.query_as_table(query)
        
        print(unique_tags)
        return unique_tags

    def _get_file_path(self, measurement_name:str, serial_name:str, time_name:str, name_mapper:str={}) -> str:

        # Get measurement name as name string
        measurement_str = measurement_name.replace(' ', '_')
        omitted_chars = "&,?!:/"
            
        for char in omitted_chars:
            measurement_str = measurement_str.replace(char, '')

        # Get device serial number as name string
        serial_str = name_mapper[serial_name.split('-')[1]] if name_mapper else serial_name.split('-')[1]
        # Get date as name string
        date_str = time_name.split('T')[0].replace('-', '')
        # Create new folder inside output folder
        super().create_folder(folder_name=date_str)
        # Path name
        file_path=f"{self.__output_directory}/{date_str}/{measurement_str}_{serial_str}_{date_str}.csv"
        
        return file_path

    def export_index_data_as_csv(self, measurement_name:str, serial_name:str, time_start:str, time_stop:str, name_mapper:dict={}) -> None:

        query = f'''
            from(bucket:"{self.__bucket}")
                |> range(start: {time_start}, stop: {time_stop})
                |> filter(fn: (r) => 
                    r["_measurement"] == "{measurement_name}" and
                    r["device_serial_number"] == "{serial_name}"
                )
            '''
        result = self.query_as_csv(query)

        path = self._get_file_path(
            measurement_name=measurement_name, 
            serial_name=serial_name, 
            time_name=time_start, 
            name_mapper=name_mapper)
        self._convert_to_dataframe_by_field(data=result, path=path)

    def export_measurement_data_as_csv(self, measurement_name:str, serial_name:str, time_start:str, time_stop:str, name_mapper:dict={}) -> None:

        query = f'''
            from(bucket:"{self.__bucket}")
                |> range(start: {time_start}, stop: {time_stop})
                |> filter(fn: (r) => 
                    r["_measurement"] == "{measurement_name}" and
                    r["device_serial_number"] == "{serial_name}" and
                    r["_field"] == "log_value"
                )
        '''
        result = self.query_as_csv(query)
        path = self._get_file_path(
            measurement_name=measurement_name, 
            serial_name=serial_name, 
            time_name=time_start, 
            name_mapper=name_mapper)
        self._convert_to_dataframe_by_tag(data=result, path=path)

    def export_data_as_csv(self, time_start:str, time_stop:str, name_mapper:dict={}) -> None:

        for serial in self.__tags["device_serial_number"]:
            for measurement in self.__measurements:
                if measurement == "data_indexes":
                    self.export_index_data_as_csv(
                        measurement_name=measurement,
                        serial_name=serial,
                        time_start=time_start, 
                        time_stop=time_stop, 
                        name_mapper=name_mapper)
                else:
                    self.export_measurement_data_as_csv(
                        measurement_name=measurement,
                        serial_name=serial,
                        time_start=time_start, 
                        time_stop=time_stop, 
                        name_mapper=name_mapper)

    def _convert_to_dataframe_by_field(self, data:csv.reader, path:str) -> None:
        df = pd.DataFrame()

        for row in data:
            if row:
                df = df.append({
                    "time": row[self.__data_structure["_time"]],
                    row[self.__data_structure["_field"]]: row[self.__data_structure["_value"]]
                    },
                    ignore_index=True
                )
        self._to_csv(df=df, path=path)

    def _convert_to_dataframe_by_tag(self, data:csv.reader, path:str) -> None:
        df = pd.DataFrame()

        for row in data:
            if row:
                df = df.append({
                    "time": row[self.__data_structure["_time"]],
                     row[self.__data_structure["parameter_name"]]: row[self.__data_structure["_value"]]
                    },
                    ignore_index=True
                )
        self._to_csv(df=df, path=path)

    def _to_csv(self, df:pd.DataFrame, path:str) -> None:

        if not df.empty:
            # Remove empty cells with empty string
            df = df.fillna('')
            # Convert 'time' column to datetime format
            df['time'] = pd.to_datetime(df['time'])
            # Set 'time' column as index
            df = df.set_index('time')
            # Convert datetime from UTC+0 to UTC+7 (Bangkok local time)
            df = df.tz_convert('Etc/GMT-7')
            # Remove timezone sign from converted datetime
            df = df.tz_localize(None)
            # group data with the same date and time
            df = df.groupby(by=["time"]).sum()
            # Export result as csv
            df.to_csv(path)


class InfluxDatabase(InfluxDatabaseQuery):

    def __init__(self, token:str, org:str, bucket:str, ipaddress:str, tag_name:str, sensor_tag_name:str, output_directory:str):
        self.token = token
        self.org = org
        self.bucket = bucket
        self.ipaddress = ipaddress
        self.tag_name = tag_name
        self.sensor_tag_name = sensor_tag_name
        self.client = InfluxDBClient(url=f"http://{self.ipaddress}:8086", token=self.token)
        self.__write_api = self.client.write_api(write_options=SYNCHRONOUS)
        super().__init__(client=self.client, org=self.org, bucket=self.bucket, output_directory=output_directory)

    def log(self, text) -> None:
        if self.logger:
            self.logger.info(text)
