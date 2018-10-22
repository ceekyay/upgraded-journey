import sys
import gzip
import os.path
import fnmatch
import json
import pandas as pd
from pandas.io.json import json_normalize
import glob

try:
    src_directory = sys.argv[1]
except:
    sys.exit("You must enter the source directory")
pattern = "*.json.gz"
concatenated_json = {}
dataframe = pd.DataFrame([])


def unpack():
    """
    :return:
    """
    for path, subdirs, files in os.walk(src_directory):
        for filename in fnmatch.filter(files, pattern):
            dest_dir = "/Users/CK/Desktop/jsonExtractor/json/" + os.path.join(filename)[:-3]
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


def __main__():
    unpack()
    for index, file in enumerate(glob.glob1(
            "/Users/CK/Desktop/jsonExtractor/json/", "*.json")):
        for index2,

if __name__ == __main__():
    __main__()