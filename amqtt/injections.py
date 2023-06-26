import json
import gpxpy
import pandas as pd
import random

file = '/Users/neerajchauhan/amqtt_mod/amqtt/scripts/Hike-2022-06-30.gpx'

def gpx_file_to_dict(filename):
    
    with open(file) as f:
        #print("File opened")
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
    #print(df.head())
    #print("End of in gpx_file_dict")
    #return df.iloc[0]
    return df

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



# def change_gpx_format_by_Lisa(data):
#     try:
#         print("I am in change_gpx lisa_format")
#         gpx_dict = gpx_str_to_dict(data.decode('utf-8'))
#         data = inject_fault_broadcast(data)
#         print(data)
#         return gpx_dict_to_str(gpx_dict).encode('utf-8')
#     except:
#         # non gpx data is not modified
#         return data
  


  
    

# # Showcase function, where data is bytecode of dictionary (written together on 4 Jule)
# def inject_fault_broadcast(data_dict):
#     # data is bytecode of dictionary
#     #data_dict = json.loads(data.decode('utf-8'))
#     print('FROM FUNCTION', type(data_dict))
#     x, y = data_dict['latitude'], data_dict['longitude']
#     x = 2*x
#     y = -y
#     data_dict['latitude'], data_dict['longitude'] = x, y
#     print("data_dict :",data_dict)
#     data = json.dumps(data_dict).encode('utf-8')
#     print('FROM FUNCTION', data)
#     return data

def change_gpx_format_by_neeraj(data):
    try:
        #print("I am in change_gpx_lisa_format")
        gpx_df = gpx_file_to_dict(file)
        #gpx_dict = gpx_file_to_dict(file)
        #print(gpx_dict)
        data = inject_fault_broadcast(gpx_df).to_dict()
        #print(data)
        return gpx_dict_to_str(data).encode('utf-8')
    except:
        # non gpx data is not modified
        return data

def inject_fault_broadcast(data_df):
    #print(data_dict)
    # data is bytecode of dictionary
    #data_dict = json.loads(data.decode('utf-8'))
    print('FROM FUNCTION', type(data_df))
    for index, row in data_df.iterrows():
        x = row['latitude']
        y = row['longitude']
        #x, y = data_dict['latitude'], data_dict['longitude']
        print("Before :", data_df.iloc[index])
        x = inject_random_number(x)
        y = inject_random_number(y)
        
        data_df.at[index, 'latitude'], data_df.at[index, 'longitude'] = x, y
        print("After :", data_df.iloc[index])
        if(index == 5):
            break
        #print("data_dict :",data_df)
        #data = json.dumps(data_dict).encode('utf-8')
        #print('FROM FUNCTION', data)
    return data_df

def inject_random_number(number):
    random_digit = random.randint(0, 9)
    number_str = str(number)

    random_position = random.randint(0, len((number_str).replace('-',''))-1)
    new_number_str = number_str[:random_position] + str(random_digit) + number_str[random_position:]

    if '.' in number_str:
        new_number = float(new_number_str)
    else:
        new_number = int(new_number_str)

    return new_number


# SNIPPET FROM FIRST TRY
# handler = self._get_handler(target_session)
# # broadcast["data"] = "hello world"                     ############################       
# print('BROADCAST IS', broadcast)                     ############################
# # function to manipulate 
# # use json.loads(a.decode('utf-8')) to decode
# broadcast['data'] = self.inject_fault_broadcast(broadcast['data'])
# print('CORRUPTED IS', broadcast)                     ############################
# task = asyncio.ensure_future(