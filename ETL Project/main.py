import glob
import pandas as pd
import xml.etree.ElementTree as et
from datetime import datetime

tmpfile="dealership_temp.tmp"
logfile="dealership_logfile.txt"
targetfile="dealership_transfomed_data.csv"

"""Reading Data as DataFrame and Returning it"""
def read_from_csv(file_to_process):
    newdf = pd.read_csv(file_to_process)
    return newdf

def read_from_json(file_to_process):
    jsdf = pd.read_json(file_to_process, lines=True)
    return jsdf

def read_from_xml (file_to_process):
    xmdf = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    tree = et.parse(file_to_process)
    root = tree.getroot()

    for person in root:
        car_model = person.find("car_model").text
        year_of_manufacturer = person.find("year_of_manufacture").text
        price = person.find("price").text
        fuel = person.find("fuel").text
        xmdf = xmdf.append({"Car_model": car_model, "Year_Of_Manufacturer": year_of_manufacturer, "Price": price, "Fuel": fuel})
        return xmdf

"""Started Extracting"""
def extract():
    extracted_data=pd.DataFrame(columns=["Car_Model", "Year_Of_Manufacture", "Price", "Fuel"])
    extracted_data1=pd.DataFrame()
    extracted_data2=pd.DataFrame()
    extracted_data3=pd.DataFrame

    # For CSV file
    for csv in glob.glob("datasource/used_car_prices1.csv"):
        extracted_data1=pd.concat(read_from_csv(csv), ignore_index=True)

    # For JSON file
    for json in glob.glob("datasource/used_car_prices1.json"):
        extracted_data2=pd.concat(read_from_json(json), ignore_index=True)

    # For xml file
    for xml in glob.glob("datasource/used_car_prices1.xml"):
        extracted_data3=pd.concat(read_from_xml(xml), ignore_index=True)

    extracted_data = pd.concat([extracted_data1, extracted_data2, extracted_data3])

    return extracted_data

""" Started Transforming the dataframe """
def transform ( data ) :
    data["price"] = round(data.price,2)
    return data

"""Started loading and logging the data"""
def load(targetfile, data_to_load):
    data_to_load.to_csv(targetfile)

# Started logging
def log(message):
    timestamp_format = "%H-%M-%S-%h-%d-%y"
    # Hour-Minute-Second-Monthname-day-year
    now=datetime.now() #get current time
    timestamp=now.strftime(timestamp_format)
    with open("dealership_logfile.txt","a") as f:
        f.write(timestamp+" , "+message+ "n")

""" Running the whole ETL Process """

# log that I have started the etl process
log("ETL job started...")

# The log to see starting and ending of that etl process
log("Extract phase started")
extracted_only_data = extract()
log("Extract phase ended")

log("Transform phase started..")
transformed_data = transform(extracted_only_data)
log("Transform phase ended..")

log("Load phase Started")
load(targetfile, transformed_data)
log("Load phase Ended")

log("ETL job ended..")


