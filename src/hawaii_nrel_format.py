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
    usage_str = "usage: %prog -p data_directory_path -o output path"
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

def format_data(infile, outpath):
    stations = sensor_data.columns
    stations_ghi = ["GHI {}".format(x) for x in stations]

    base = os.path.basename(infile)
    day = os.path.splitext(base)[0]
    outfile = "{}/{}.csv".format(outpath, day)

    df = pd.read_csv(infile, header = None, names = data_columns)
    df = df.round({s: 4 for s in stations_ghi})
    time = get_datetime_series(df[["Seconds", "Year", "DOY", "HST"]])

    data = pd.DataFrame({'datetime': time})
    data = pd.concat([data, df[stations_ghi]], axis = 1)
    data.to_csv(outfile, header = True, index = False)

def format_data_unpack(arg):
    format_data(*arg)

if __name__=="__main__":
    options, args = parse_options()

    infiles = get_input_files(options.path)

    if not os.path.exists(options.outpath):
        os.makedirs(options.outpath)

    num_cores = mp.cpu_count()
    with mp.Pool(num_cores) as p:
        args = [(inf, options.outpath) for inf in infiles]
        list(tqdm(p.imap(format_data_unpack, args), total = len(infiles)))



