import json
import os
import pandas as pd
import iso8601

def load_json(fname):
    with open(fname, 'r') as f:
        j = json.load(f)
    return j

def get_csv_files_list(path):
    files = []
    for f in os.listdir(path):
        if f.endswith(".csv"):
            files.append("{}/{}".format(path, f))
    return files

def read_solarnc_csv(infile, tzone):
    df = pd.read_csv(infile, index_col = "datetime", parse_dates=True)
    df.index = df.index.tz_localize('UTC').tz_convert(tzone)
    return df

def save_solarnc_csv(df, outfile):
    df.to_csv(outfile, header = True, index = True)

#def sample_audit(time_data):


def get_consecutive_falses(ser):
    aux = ser | (ser != ser.shift())
    groups = ser.groupby(aux.cumsum()).transform('count')
    groups[ser] = 0
    return groups

def ghi_audit(df):
    ghi_df = df.filter(regex=('GHI *'))
    return ghi_df.apply(lambda x: get_consecutive_falses(x >= 0).max())


