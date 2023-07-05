import json
import gpxpy
import pandas as pd

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