# -*- coding: utf-8 -*-
import solarnc as snc
import optparse
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
    return (options, args)


# TODO obtener fecha comienzo de la muestra de datos, crear variable a las 0:00 de ese día
# TODO obtener los datos de esa marca temporal para cada una de las estaciones, incrementar en minutos
# TODO filtrar datos con valores físicamente imposible, según las indicaciones de la BSRN (mirar en el libro)
# TODO generar log con los datos que se han filtrado y un código de error que indique el motivo
# TODO incluir en el log los momentos para los que alguna estación no tiene datos registrados
# TODO obtener datos de elevación solar y eliminar los registros con una elevación inferior a 7º


def main(options, args):
    config = snc.load_config(options.config)
    print("Correct config format")

    dtset = config['dataset']
    print("Dataset: {}".format(dtset['name']))
    path = dtset['path']
    print("Input files from {}".format(path))

    fconfig = config['format']
    outpath = fconfig['outpath']
    print("Output files to {}".format(outpath))
    outpath = fconfig['outpath']
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    stations = fconfig['stations']
    print("Selected stations:")
    print(stations)


if __name__ == "__main__":
    main(*parse_options())
