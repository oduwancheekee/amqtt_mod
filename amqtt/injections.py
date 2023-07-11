import json
import gpxpy
import pandas as pd
import numpy as np

def redundancy_injection(message):
    # message = '2022-06-30 18:03:22+00:00 49 -140 101'
    # output = '2022-06-30 18:03:22+00:00 49 INJ(49) -140 INJ(-140) -140 101'
    parts = message.split()
    lon = float(parts[2])
    lat = float(parts[3])
    
    low_log = -3
    high_log = -2
    med_log = (low_log + high_log)/2
    
    prob_to_inject = 0.05
    uniform_a = np.random.rand() 
    
    if uniform_a > 1-prob_to_inject:
        lon2 = lon + 10**np.random.uniform(low_log, high_log) - 10**med_log
        lat2 = lat + 10**np.random.uniform(low_log, high_log) - 10**med_log
    elif uniform_a < prob_to_inject:
        lon2 = lon
        lat2 = lat
        parts[2] = str(lon + 10**np.random.uniform(low_log, high_log) - 10**med_log)
        parts[3] = str(lat + 10**np.random.uniform(low_log, high_log) - 10**med_log)
    else:
        lon2 = lon
        lat2 = lat

    parts.insert(4, str(lat2))
    parts.insert(3, str(lon2))
    return ' '.join(parts)


def gpx_file_to_dict(filename):
    with open(filename) as f:
        gpx = gpxpy.parse(f)
        points = []
    for segment in gpx.tracks[0].segments:
        for p in segment.points:
            points.append({
                'time': p.time,
                'latitude': p.latitude,
                'longitude': p.longitude,
                'elevation': p.elevation,
            })
    df = pd.DataFrame.from_records(points)
    return df.to_dict()


def gpx_str_to_dict(f):
    gpx = gpxpy.parse(f)
    points = []
    for segment in gpx.tracks[0].segments:
        for p in segment.points:
            points.append({
                'time': p.time,
                'latitude': p.latitude,
                'longitude': p.longitude,
                'elevation': p.elevation,
            })
    df = pd.DataFrame.from_records(points)
    return df.to_dict()


def gpx_dict_to_file(gps_dict):
    df_conv=pd.DataFrame.from_dict(gps_dict, orient='columns')
    gpx = gpxpy.gpx.GPX()
    gpx_track = gpxpy.gpx.GPXTrack()
    gpx.tracks.append(gpx_track)
    gpx_segment = gpxpy.gpx.GPXTrackSegment()
    gpx_track.segments.append(gpx_segment)
    for i in range(len(df_conv)):
        conv_time = pd.Timestamp(df_conv['time'][i])
        gpx_segment.points.append(gpxpy.gpx.GPXTrackPoint(df_conv['longitude'][i], df_conv['latitude'][i], elevation=df_conv['elevation'][i], time=conv_time))
    with open("output.gpx", "w") as f:
        f.write(gpx.to_xml())


def gpx_dict_to_str(gps_dict):
    df_conv=pd.DataFrame.from_dict(gps_dict, orient='columns')
    gpx = gpxpy.gpx.GPX()
    gpx_track = gpxpy.gpx.GPXTrack()
    gpx.tracks.append(gpx_track)
    gpx_segment = gpxpy.gpx.GPXTrackSegment()
    gpx_track.segments.append(gpx_segment)
    for i in range(len(df_conv)):
        conv_time = pd.Timestamp(df_conv['time'][i])
        gpx_segment.points.append(gpxpy.gpx.GPXTrackPoint(df_conv['longitude'][i], df_conv['latitude'][i], elevation=df_conv['elevation'][i], time=conv_time))
    return gpx.to_xml()


def str_to_dict(_str):                                  #subscriber parser (Lisa)
    _list = _str.split('|')
    i = 0
    _dict = {}
    _dict['time'] = []
    _dict['latitude'] = []
    _dict['longitude'] = []
    _dict['elevation'] = []
    while i < len(_list):
        _tmp_list = _list[i].split()
        
        _dict['time'].append(_tmp_list[0]+ " " + _tmp_list[1])
        _dict['latitude'].append(_tmp_list[2])
        _dict['longitude'].append(_tmp_list[3])
        _dict['elevation'].append(_tmp_list[4])
        i = i+1 
    return _dict


def dict_to_df(_dict):                                  # for visualisation parser (Lisa)                   
    df = gpd.GeoDataFrame.from_records(_dict)
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
    for i in range (0,len(gdf)):
        conv_time = pd.Timestamp(gdf['time'][i])
        gdf['time'][i] = conv_time
    return gdf


def gpx_array_to_file(longitude, latitude, time, elevation):
    longitude, latitude, time, elevation = (np.array(x) for x in (longitude, latitude, time, elevation))
    gpx = gpxpy.gpx.GPX()
    #Create first track in GPX file:
    gpx_track = gpxpy.gpx.GPXTrack()
    gpx.tracks.append(gpx_track)

    # Create first segment in GPX track:
    gpx_segment = gpxpy.gpx.GPXTrackSegment()
    gpx_track.segments.append(gpx_segment)

    # loop over points and add to segment
    for i in range(len(longitude)):
        gpx_segment.points.append(gpxpy.gpx.GPXTrackPoint(longitude=longitude[i], latitude=latitude[i], elevation=elevation[i], time=time[i]))

    # write gpx trace as file
    with open('output.gpx','w')as f:
        f.write(gpx.to_xml())


def change_gpx_format(data):
    try:
        gpx_dict = gpx_str_to_dict(data.decode('utf-8'))
        return gpx_dict_to_str(gpx_dict).encode('utf-8')
    except:
        # non gpx data is not modified
        return data
