import glob  # this module helps in selecting files
import pandas as pd  # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime

""" Set Paths"""
tmpFile = r"D:\ETL Project Coursera\temp.tmp"  # file used to store all extracted data
logfile = r"D:\ETL Project Coursera\logfile.txt"  # all event logs will be stored in this file
targetFile = r"D:\ETL Project Coursera\transformed_data.csv"  # file where transformed data is stored


def extract_from_csv(file_to_process):
    """ CSV extract function"""
    df = pd.read_csv(file_to_process)
    return df


def extract_from_json(file_to_process):
    """ JSON extract function"""
    df = pd.read_json(file_to_process)
    return df


def extract_from_xml(file_to_process):
    """ PARQUET extract function"""
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = dataframe.append({"name": name, "height": height, "weight": weight}, ignore_index=True)
    return dataframe


def extract():
    """ All dataFrame will be appended into one file"""
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])  # create an empty dataFrame to hold the
    # extracted data

    # process all the csv file into one dataFrame
    for csv_files in glob.glob("D:\ETL Project Coursera\source\*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csv_files), ignore_index=True)

    # process all the json file into the previous dataFrame where all the csv data is already present
    for json_files in glob.glob("D:\ETL Project Coursera\source\*.json"):
        extracted_data = extracted_data.append(extract_from_json(json_files), ignore_index=True)

    # process all the xml file into the previous dataFrame where all the csv and json data is already present
    for xml_files in glob.glob("D:\ETL Project Coursera\source\*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xml_files), ignore_index=True)

    return extracted_data


def transform(data):
    """ converting the height which is in inches to millimeter round off to two decimeter"""
    """ converting the weight which is in pounds to kilogram round off to two decimeter"""
    data["height"] = round(data.height * 0.0254, 2)
    data["weight"] = round(data.weight * 0.45359237, 2)
    return data


def load(targetFile, data_to_load):
    data_to_load.to_csv(targetFile)


def log(message):
    """ log message for debugging the code.."""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')


""" Running the ETL process.."""
log("ETL Job Strated.....")

log("Extract phase Started")
extracted_data = extract()  # calling extract function
log("Extract phase Ended")
# extracted_data

log("Transform phase Started")
transformed_data = transform(extracted_data)  # calling transform function
log("Transform phase Ended")
# transformed_data

log("Load phase Started")
load(targetFile, transformed_data)  # putting data into target-file
log("Load phase Ended")

log("ETL Job Ended.....")