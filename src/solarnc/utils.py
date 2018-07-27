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

def read_solarnc_csv(infile):
    df = pd.read_csv(infile, index_col = None)
    # FIXME: using pd.to_datetime is faster (20%) but timezone info is lost
    # I do not now any better solution than using the iso8601 parse_date parser
    #df['datetime'] = pd.to_datetime(df['datetime'])
    df['datetime'] = df['datetime'].apply(iso8601.parse_date)
    return df

def save_solarnc_csv(df, outfile):
    df.to_csv(outfile, header = True, index = False)
