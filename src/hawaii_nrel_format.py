#!/usr/bin/env python

import pandas as pd
import optparse
import os
import multiprocessing as mp
from tqdm import tqdm

data_columns = ["Seconds", "Year", "DOY", "HST", "GHI DH3", "GHI DH4",
	"GHI DH5", "GHI DH10", "GHI DH11", "GHI DH9", "GHI DH2", "GHI DH1",
	"Global Tilt DH1", "GHI AP6", "Global Tilt AP6", "GHI AP1", "GHI AP3",
	"GHI AP5", "GHI AP4", "GHI AP7", "GHI DH6", "GHI DH7", "GHI DH8"]

drop_columns = ["GHI AP3"]

sensor_data = pd.DataFrame({
	"AP1": [21.31276, -158.08389],
	"AP3": [21.31281, -158.08163],
	"AP4": [21.31141, -158.07947],
	"AP5": [21.30983, -158.08249],
	"AP6": [21.30812, -158.07935],
	"AP7": [21.31478, -158.07785],
	"DH1": [21.31533, -158.087],
	"DH2": [21.31451, -158.08534],
	"DH3": [21.31236, -158.08463],
	"DH4": [21.31303, -158.08505],
	"DH5": [21.31357, -158.08424],
	"DH6": [21.31179, -158.08678],
	"DH7": [21.31418, -158.08685],
	"DH8": [21.31034, -158.08675],
	"DH9": [21.31268, -158.08688],
	"DH10": [21.31183, -158.08554],
	"DH11": [21.31042, -158.0853]
        }, index = ["latitude", "longitude"])

def parse_options():
    usage_str = '''usage: %prog -p data_directory_path -o output path

    It reads al the txt files from the data_directory_path, were one file per
    day is expected, with the samples of each NREL station for that day. It
    writes one output file per day in the output path with the formated data.

    The outputs are csv files, where the first column is in datetime localized
    format (sample instant), and the rest of the columns contain the GHI values,
    each column representing one of the NREL stations.
    '''

    parser = optparse.OptionParser(usage_str)
    parser.add_option("-p", "--path", dest="path", type="string",
            help="path to the raw data directory")
    parser.add_option("-o", "--out", dest="outpath", type="string",
            help="output directory")

    options, args  = parser.parse_args()

    if not options.path:
        parser.error("missing path")

    if not options.outpath:
        parser.error("missing outpath")

    return (options, args)


def get_input_files(path):
    files = []
    for f in os.listdir(path):
        if f.endswith(".txt"):
            files.append("{}/{}".format(path, f))
    return files

def get_datetime_series(df):
    def date2str(ser):
        sec = ser["Seconds"]
        year = ser["Year"]
        doy = ser["DOY"]
        hst = ser["HST"]
        return "{} {} {} {}".format(sec, year, doy, hst)

    time_df = df.apply(date2str, axis = "columns")
    time_df = pd.to_datetime(time_df, format='%S %Y %j %H%M')
    # HST is the name for the hawaii time zone
    return time_df.dt.tz_localize('HST')

def save_file(data, path, day):
    outfile = "{}/{}.csv".format(path, day)
    data.to_csv(outfile, header = True, index = False)

def drop_failing_stations(df, stations_ghi):
    df.drop(columns = drop_columns, inplace = True)
    for col in drop_columns:
        stations_ghi.remove(col)

def filter_hours(df):
    drop_rows = (df['HST'] < 730) | (df['HST'] > 1730)
    df.drop(df[drop_rows].index, inplace = True)

def filter_negatives(df, stations_ghi):
    reject = False
    for sta in stations_ghi:
        neg_values = (df[sta] < 0.0).sum()
        #print('neg_values: {}'.format(neg_values))
        if neg_values < 0.1 * df.shape[0]:
            df.loc[df[sta] < 0, sta] = 0
        else:
            reject = True
    return reject

def format_data(infile, outpath):
    stations = sensor_data.columns
    stations_ghi = ["GHI {}".format(x) for x in stations]

    base = os.path.basename(infile)
    day = os.path.splitext(base)[0]
    df = pd.read_csv(infile, header = None, names = data_columns)

    drop_failing_stations(df, stations_ghi)

    filter_hours(df)

    df = df.round({s: 4 for s in stations_ghi})
    time = get_datetime_series(df[["Seconds", "Year", "DOY", "HST"]])
    data = pd.DataFrame({'datetime': time})
    data = pd.concat([data, df[stations_ghi]], axis = 1)

    if filter_negatives(df, stations_ghi):
        outpath = '{}_reject'.format(options.outpath)
    save_file(data, outpath, day)

def format_data_unpack(arg):
    format_data(*arg)

if __name__=="__main__":
    options, args = parse_options()
    infiles = get_input_files(options.path)
    rejected_files = {}

    if not os.path.exists(options.outpath):
        os.makedirs(options.outpath)

    outpath_reject = '{}_reject'.format(options.outpath)
    if not os.path.exists(outpath_reject):
        os.makedirs(outpath_reject)

    # comment this for debug
    num_cores = mp.cpu_count()
    with mp.Pool(num_cores) as p:
        args = [(inf, options.outpath) for inf in infiles]
        list(tqdm(p.imap(format_data_unpack, args), total = len(infiles)))

    # uncoment this for debug
    #for inf in infiles:
    #    format_data(inf, options.outpath)
