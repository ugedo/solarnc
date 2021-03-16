# -*- coding: utf-8 -*-
from datetime import timedelta
from datetime import datetime
import solarnc as snc
import pandas as pd
import numpy as np
import optparse
import glob
import sys
import os


def parse_options():
    usage_str = "%prog -c json_config_file "#[-j num_jobs]"
    parser = optparse.OptionParser(usage_str)
    parser.add_option("-c", dest="config", type="string",
            help="path to the json file with the configuration parameters")
#    parser.add_option("-j", dest="npjobs", type="int",
#            help="number of parallel jobs to use", default = mp.cpu_count())

    options, args = parser.parse_args()
    if not options.config:
        parser.error("missing json config file")
    return options, args


def import_metas_data(infiles, flog, columns_selected):
    horasluz = {}
    for d in infiles:
        df = snc.read_csv(d, [1, 2, 3])
        # df.droplevel([1, 2], axis='columns')
        columns_selected_metas = ['Elevacion'] + columns_selected
        df = df[columns_selected_metas]
        df = df[df[('Elevacion', 'deg', 'Avg')] > 7]
        amanecer = min(df.index)
        ocaso = max(df.index)
        dia = amanecer.date()
        if dia not in horasluz:
            horasluz[dia] = (amanecer, ocaso)
        else:
            horasluz[dia] = (min(amanecer, horasluz[dia][0]), max(ocaso, horasluz[dia][1]))
        if df.loc[amanecer][('Elevacion', 'deg', 'Avg')] > 7.2 or \
                df.loc[ocaso][('Elevacion', 'deg', 'Avg')] > 7.2:
            print(5, amanecer, ocaso, file=flog, sep=',')
            print('Error en amanecer/ocaso', amanecer, ocaso, file=sys.stderr)
        df.drop(columns=["Elevacion"], level=0, inplace=True)

        if 'df_station' not in locals():
            df_station = pd.DataFrame(columns=df.columns)
        df_station = pd.concat([df_station, df])

    return horasluz, df_station


def import_station_data(infiles, s, horasluz, flog, columns_selected):
    for d in infiles:
        df = snc.read_csv(d, [1, 2, 3])
        df = df[columns_selected]
        dia = df.index[1].date()

        if 'df_station' not in locals():
            df_station = pd.DataFrame(columns=df.columns)

        if dia not in horasluz:
            print(6, dia, s, file=flog, sep=',')
            print('Geometría solar no disponible para el día', dia, file=sys.stderr)
        else:
            df = df[(df.index >= horasluz[dia][0]) & (df.index <= horasluz[dia][1])]
            df_station = pd.concat([df_station, df])

    return df_station


def minutar(df, fecha_i, fecha_fin, horasluz, flog):
    print('Recopilando datos minutales')
    while fecha_i <= fecha_fin:
        dia = fecha_i.date()
        while fecha_i <= horasluz[dia][1]:
            if fecha_i not in df.index:
                print(4, fecha_i, file=flog, sep=',')
                print('Error en la hora', fecha_i, file=sys.stderr)
            fecha_i += np.timedelta64(1, 'm')
        fecha_i = horasluz.get(dia + timedelta(days=1), (fecha_i, fecha_i))[0]


# TODO filtrar datos con valores físicamente imposible, según las indicaciones de la BSRN (mirar en el libro)
# TODO generar log con los datos que se han filtrado y un código de error que indique el motivo
# TODO incluir en el log los momentos para los que alguna estación no tiene datos registrados


def main(options, args):
    config = snc.load_config(options.config)
    print("Correct config format")

    dtset = config['dataset']
    print("Dataset: {}".format(dtset['name']))

    rconfig = config['rawer']
    path = rconfig['inpath']
    print("Input files from {}".format(path))
    outpath = rconfig['outpath']
    print("Output files to {}".format(outpath))
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    stations = rconfig['stations']
    print("Selected stations:")
    print(stations)

    columns_selected = rconfig['columns']
    print("Selected columns:")
    print(columns_selected)

    current_file = os.path.realpath(__file__)
    schema_path = os.path.dirname(current_file)
    schema_fname = "{}/../jsons/solarnc_schema.json".format(schema_path)

    # Se crea y abre fichero de log
    flog = datetime.now()
    flog = os.path.dirname(current_file) + "/../logs/" + flog.strftime('%Y-%m-%d_%H%M') + '.txt'
    if not os.path.exists(os.path.dirname(flog)):
        os.makedirs(flog)
    flog = open(flog, 'w')
    print('Loging...')
    print('0', datetime.now().strftime('%H:%M:%S.%f'), file=flog, sep=',')

    # Se crea diccionario de fechas asociando la estación, fecha y fichero (de datos de esa estación para esa fecha)
    df_stations = {}
    print('Reading METAS data')
    infiles = glob.glob("{}/../{}METAS/Minutos/*.dat".format(schema_path, path))
    horasluz, df_metas = import_metas_data(infiles, flog, columns_selected)
    print('Reading stantions data')
    for s in stations:
        if s != 'METAS':
            print('   ' + s)
            infiles = glob.glob("{}/../{}{}/Minutos/*.dat".format(schema_path, path, s))
            df_stations[s] = import_station_data(infiles, s, horasluz, flog, columns_selected)
            df_metas = df_metas.join(df_stations[s], rsuffix=str('_' + s))
            # Una vez concatenado se elimina para ahorrar espacio en memoria
            df_stations.pop(s)

        # Se obtiene fecha comienzo de la muestra de datos de la estación
        # print('Acotando las fechas de la muestra...')
        # minima = min(df_stations[s].index) if 'minima' not in locals() else min(min(df_stations[s].index), minima)
        # maxima = max(df_stations[s].index) if 'maxima' not in locals() else max(max(df_stations[s].index), maxima)

    # minutar(df_stations['METAS'], minima, maxima, horasluz, flog)

    outname = "{}/../{}/".format(schema_path, outpath)
    print('Exportando a CSV')
    df_metas.to_csv(str(outpath + '/rawGlobalOnly.dat'), header=False)
    # Exportar a fichero de texto .txt
    print('Exportando a TXT')
    f = open(str(outpath + '/rawGlobalOnly.txt'), 'w')
    f.write(df_metas.to_string())
    f.close()

    print('0', datetime.now().strftime('%H:%M:%S.%f'), file=flog, sep=',')
    flog.close()


if __name__ == "__main__":
    main(*parse_options())
