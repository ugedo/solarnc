import pandas as pd
import numpy as np
import sys
import os


if __name__ == '__main__':
    # Crea un mapa que asocia una fecha AAAA-MM-DD a una ruta del sistema
    # la ruta es el fichero/s pasado como argumento, que contiene las rutas de los .dat de ese día
    # una línea para cada uno de los sensores, de ese mismo día y misma periodicidad
    fechas = {}
    for i, f in enumerate(sys.argv[1:]):
        base = os.path.basename(f)
        name = base.split('_')
        name[4] = '0' + name[4] if int(name[4]) < 10 else name[4]
        name[3] = '0' + name[3] if int(name[3]) < 10 else name[3]
        # fecha = np.datetime64(name[2] + '-' + name[3] + '-' + name[4])
        fecha = name[2] + '-' + name[3] + '-' + name[4]
        if fecha not in fechas:
            fechas[fecha] = f
        else:
            print('Se han encontrado ficheros duplicadaos en la posición ', i,
                  ':\n    -x ' + f + '\n    -> ' + fechas[fecha])
    ordenadas = sorted(fechas)
    iter = 0
    df_diario = []
    for d in ordenadas:
        with open(fechas[d], 'r') as f:
            sensors = []
            for linea in f:
                sensors.append(linea)
        for s in sensors:
            df = pd.read_csv(s, header=[1, 2, 3])
            df = df.iloc[:, [0, 3]]
            print(df.to_string(header=False, index=False))
            # TODO extraer el segundo, año, dia juliano y tercer parámetro (descubrir qué es)
            # TODO añadir a cada fila los distintos datos de cada sensor
            # TODO exportar a un txt como CSV
