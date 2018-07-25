import pysolar as psol
import math

def __csm_guillermo(dtime, latitude, longitude):
    # Are these specific to HAWAII????
    AM = 2.0     # Default air mass is 2.0
    TL = 1.0     # Default Linke turbidity factor is 1.0
    SC = 1367.0  # Solar constant in W/m^2 is 1367.0.
    TY = 365     # Total year number from 1 to 365 days

    KD = psol.util.mean_earth_sun_distance(dtime)
    DEC = psol.util.declination_degree(dtime, TY)

    #Optical Depth:
    doy = dtime.timetuple().tm_yday
    OD = psol.radiation.get_optical_depth(doy)

    altitude = psol.solar.get_altitude(latitude, longitude, dtime)

    #FSOLALT (Page model):
    FSOLALT = 0.038175 + 1.5458 * math.sin(math.radians(altitude)) \
            - 0.5998 * math.sin(math.radians(altitude)) ** 2

    #Diffuse Transmitance:
    DT = ((-21.657) + (41.752 * (TL)) + (0.51905 * (TL) * (TL)))

    #Direct Irradiation under clear: DIRC:
    DIRC = SC * KD * math.exp(-0.8662 * AM * TL * OD) \
            * math.sin(math.radians(altitude))

    #Maybe the reference of the altitude has a phase shift
    #Diffuse Irradiation under clear sky: DIFFC:
    DIFFC = KD * DT * FSOLALT

    return abs(DIRC) + abs(DIFFC)


def csm_guillermo(df, stations):
    stalist = list(stations.keys())
    for sta in stalist:
        if "latitude" not in stations[sta]:
            raise ValueError('No latitude for station {}'.format(sta))

        if "longitude" not in stations[sta]:
            raise ValueError('No longitude for station {}'.format(sta))

        latitude = stations[sta]["latitude"]
        longitude = stations[sta]["longitude"]

        csm_fun = lambda x: __csm_guillermo(x, latitude, longitude)
        csm = df['datetime'].apply(csm_fun)
        ghi_rel = df['GHI {}'.format(sta)] / csm
        header = 'GHInorm {}'.format(sta)
        df[header] = ghi_rel
