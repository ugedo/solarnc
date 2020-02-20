from pvlib.location import Location

# pvlib provides: ‘ineichen’, ‘haurwitz’, ‘simplified_solis'
def csm_pvlib(df, stations, model, itype):
    for sta in stations:
        loc = Location(sta["latitude"], sta["longitude"])
        name = "{} {} {}".format(model, itype, sta["name"])
        df[name] = loc.get_clearsky(df.index, model = model)[itype]
