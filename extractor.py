import gzip
import os.path
import fnmatch
import json
import pandas as pd
from pandas.io.json import json_normalize
import glob

root_dir = '/Users/CK/Desktop/jsonExtractor/2016'
pattern = '*.json.gz'

for path, subdirs, files in os.walk(root_dir):
    for filename in fnmatch.filter(files, pattern):
        #print (os.path.join(path, filename))
        # dest_dir = "/Users/CK/Desktop/jsonExtractor/"+os.path.join(filename)[:-3]
        dest_dir = "/Users/CK/Desktop/jsonExtractor/json/"+os.path.join(filename)[:-3]
        with gzip.open(os.path.join(path, filename),'rb') as infile:
            with open(dest_dir, 'wb') as outfile:
                for line in infile:
                    outfile.write(line)


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


# Create a new python dataframe
df_combined = pd.DataFrame([])
# Count number of text files in Data folder into a variable
Counter = len(glob.glob1("/Users/CK/Desktop/jsonExtractor/json/","*.json"))
print(Counter)
i = 1
for infile in glob.glob1("/Users/CK/Desktop/jsonExtractor/json/","*.json"):
    if i <= (Counter):
        data_parsed = json.loads(open("/Users/CK/Desktop/jsonExtractor/json/"+infile).read())
        Object = data_parsed['Records']
        j = 0
        while j<len(Object):
            print("_______________")
            print(Object[j])
            flat = flatten_json(Object[j])
            #print(flat)
            df = json_normalize(flat)
            #print(df)
            df_combined = df_combined.append(df)
            print(j)
            j += 1
            print(j)

df_combined.to_csv("/Users/CK/Desktop/jsonExtractor/Newfile.csv", index=False)


print(df_combined.shape)

