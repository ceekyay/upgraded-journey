import pandas as pd
import random
import time

df = pd.DataFrame([])
total_rows = 0


def read_data(src):
    """
    :param src: System Path of where the json files are
    :return:
    """
    global df, total_rows
    df = pd.read_csv(src)
    total_rows = len(df.index)


def indices_lottery():
    """ Randomly picks which rows to add for the 70% (major_df) data and
    30% (minor_df)
    :return:
    """
    minor_num = int(total_rows * 0.30)
    minor_indices = random.sample(range(0, total_rows), minor_num)
    minor_indices = set(minor_indices)
    major_indices = set()
    for index in range(0, total_rows):
        if index not in minor_indices:
            major_indices.add(index)
    return major_indices, minor_indices


def split_data():
    """Splits the main data into 2 parts. One part(70%) goes into major_df
    and the other (30%) into minor_df
    :return:
    """
    major_indices, minor_indices = indices_lottery()
    column_titles = list(df.columns.values)
    major_df = pd.DataFrame([], columns=column_titles)
    minor_df = pd.DataFrame([], columns=column_titles)

    major_index = 0
    minor_index = 0
    for index in range(total_rows):
        if index in minor_indices:
            minor_df.loc[minor_index] = df.iloc[index]
            minor_index += 1
        else:
            major_df.loc[major_index] = df.iloc[index]
            major_index += 1
    return major_df, minor_df


def __main__():
    """
    :return:
    """
    start = time.time()
    src = "/Users/CK/Desktop/jsonExtractor/Newfile.csv"
    read_data(src)
    major_df, minor_df = split_data()
    major_df.to_csv("/Users/CK/Desktop/jsonExtractor/70.csv", index=False)
    minor_df.to_csv("/Users/CK/Desktop/jsonExtractor/30.csv", index=False)
    end = time.time()
    print("TOTAL TIME ELAPSED: ", end-start)


if __name__ == __main__():
    __main__()
