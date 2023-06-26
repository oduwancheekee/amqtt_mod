import json
import gpxpy
import pandas as pd
import numpy as np

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

def gpx_file_to_numpy_GPSClean(filename):
    # output format for GPSClean 
    with open(filename) as f:
        gpx = gpxpy.parse(f)
    points = []
    times = []
    for track in gpx.tracks: 
        for segment in track.segments:
            for point in segment.points:
                points.append(np.array([point.longitude, point.latitude, point.elevation, point.time]))
                times.append(point.time)
    points = np.array(points)
    times = np.array(times)
    return points, times

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



def change_gpx_format_by_Lisa(data):
    try:
        gpx_dict = gpx_str_to_dict(data.decode('utf-8'))
        return gpx_dict_to_str(gpx_dict).encode('utf-8')
    except:
        # non gpx data is not modified
        return data

# Showcase function, where data is bytecode of dictionary (written together on 4 Jule)
def inject_fault_broadcast(data):
    # data is bytecode of dictionary
    data_dict = json.loads(data.decode('utf-8'))
    print('FROM FUNCTION', type(data_dict))
    x, y = data_dict['lat'], data_dict['lon']
    x = 2*x
    y = -y
    data_dict['lat'], data_dict['lon'] = x, y
    data = json.dumps(data_dict).encode('utf-8')
    print('FROM FUNCTION', data)
    return data


# SNIPPET FROM FIRST TRY
# handler = self._get_handler(target_session)
# # broadcast["data"] = "hello world"                     ############################       
# print('BROADCAST IS', broadcast)                     ############################
# # function to manipulate 
# # use json.loads(a.decode('utf-8')) to decode
# broadcast['data'] = self.inject_fault_broadcast(broadcast['data'])
# print('CORRUPTED IS', broadcast)                     ############################
# task = asyncio.ensure_future(