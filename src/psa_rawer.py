# -*- coding: utf-8 -*-
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


# TODO filtrar datos con valores físicamente imposible, según las indicaciones de la BSRN (mirar en el libro)
# TODO generar log con los datos que se han filtrado y un código de error que indique el motivo
# TODO incluir en el log los momentos para los que alguna estación no tiene datos registrados
# TODO obtener datos de elevación solar y eliminar los registros con una elevación inferior a 7º


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
    infiles = {}
    horasluz = ([], [])
    print('Reading stantions data')
    for s in stations:
        infiles[s] = glob.glob("{}/../{}{}/Minutos/*.dat".format(schema_path, path, s))

        # DONE añadir la opción de que para un mismo día exista más de un fichero
        for d in infiles[s]:
            df = snc.read_csv(d, [1, 2, 3])
            df = df[df[('Elevacion', 'deg', 'Avg')] > 7]
            horasluz[0].append(min(df.index))
            horasluz[1].append(max(df.index))
            if s not in df_stations:
                columnas = df.columns
                df_stations[s] = pd.DataFrame(columns=columnas)
            df_stations[s] = pd.concat([df_stations[s], df])

        # Se obtiene fecha comienzo de la muestra de datos de la estación
        print('Acotando las fechas de la muestra...')
        minima = min(df_stations[s].index) if 'minima' not in locals() else min(min(df_stations[s].index), minima)
        maxima = max(df_stations[s].index) if 'maxima' not in locals() else max(max(df_stations[s].index), maxima)

    horasluz[0].sort()
    horasluz[1].sort()

    # Se crea variable a las 0:00 del día inicio de la muestra a las 0:00 y final a las 23:59
    fecha_i = minima# + np.timedelta64(0, 'm')
    fecha_fin = maxima #np.datetime64(maxima + 1) - np.timedelta64(1, 'm')
    ffechas = os.path.dirname(os.path.realpath(__file__)) + "/../logs/" + 'fechas.txt'
    ffechas = open(ffechas, 'w')
    df = df_stations['METAS']
    # df.set_index(columnas[0], inplace=True)
    print('Recopilando datos minutales')
    while fecha_i <= fecha_fin:
        if fecha_i not in df.index:
            if fecha_i - np.timedelta64(1, 'm') in horasluz[1]:
                fecha_i = horasluz[0][horasluz[1].index(fecha_i - np.timedelta64(1, 'm')) + 1]
            else:
                print(4, fecha_i, file=flog, sep=',')
                print('Error en la hora ', fecha_i, file=sys.stderr)
        fecha_i += np.timedelta64(1, 'm')
        # TODO obtener los datos de esa marca temporal para cada una de las estaciones, incrementar en minutos

    print('0', datetime.now().strftime('%H:%M:%S.%f'), file=flog, sep=',')
    flog.close()
    ffechas.close()


if __name__ == "__main__":
    main(*parse_options())
