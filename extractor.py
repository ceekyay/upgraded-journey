import time
import gzip
import os.path
import fnmatch
import json
import pandas as pd
from pandas.io.json import json_normalize
import glob

BASE_FILEPATH = "/Users/CK/Documents/8K/upgraded-journey/"
data_source = BASE_FILEPATH + "Data/"
data_destination = BASE_FILEPATH + "Data/json/"
pattern = "*.json.gz"
concatenated_json = {}
dataframe = pd.DataFrame([])


def unpack():
    """
    :return:
    """
    for path, subdirs, files in os.walk(data_source):
        for filename in fnmatch.filter(files, pattern):
            dest_dir = "/Users/CK/Documents/8K/upgraded-journey/Data/json/" \
                       + os.path.join(filename)[:-3]
            with gzip.open(os.path.join(path, filename), 'rb') as infile:
                with open(dest_dir, 'wb') as outfile:
                    for line in infile:
                        outfile.write(line)


def gen_new_concatenated_json(data):
    """
    :param data:
    :return:
    """
    global concatenated_json
    concatenate_child_attributes_to_parent(child_data=data)
    return concatenated_json


def concatenate_child_attributes_to_parent(child_data=None,
                                           parent_attribute=""):
    """
    :param child_data:
    :param parent_attribute:
    :return:
    """
    global concatenated_json
    if isinstance(child_data, dict):
        for attribute in child_data:
            # Recursion: If even the "attribute" in child_data has more nesting.
            concatenate_child_attributes_to_parent(child_data[attribute],
                                                   parent_attribute
                                                   + attribute + "_")
    elif isinstance(child_data, list):
        for index, attribute in enumerate(child_data):
            concatenate_child_attributes_to_parent(attribute,
                                                   parent_attribute
                                                   + str(index) + "_")
    else:
        concatenated_json[parent_attribute[:-1]] = child_data


def write_to_csv(csv_name):
    csv_location = BASE_FILEPATH + csv_name + ".csv"
    dataframe.to_csv(csv_location, index=False)


def __main__():
    start = time.time()
    unpack()
    for infile in glob.glob1(data_destination, "*.json"):
        data_parsed = json.loads(open(data_destination + infile).read())
        parent_data = data_parsed['Records']
        for count in range(len(parent_data)):
            modified_data = gen_new_concatenated_json(parent_data)
            dataframe.append(json_normalize(modified_data))
    end_collection = time.time()
    print("DATA COLLECTION TIME: ", end_collection-start)
    write_to_csv("main_data")
    end = time.time()
    print("DATA WRITE TIME: ", end_collection-end)


if __name__ == __main__():
    __main__()
