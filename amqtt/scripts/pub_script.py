# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
"""
amqtt_pub - MQTT 3.1.1 publisher

Usage:
    amqtt_pub --version
    amqtt_pub (-h | --help)
    amqtt_pub --url BROKER_URL -t TOPIC (-f FILE | -l | -m MESSAGE | -n | -s | --whole-file FILE) [-c CONFIG_FILE] [-i CLIENT_ID] [-q | --qos QOS] [-d] [-k KEEP_ALIVE] [--clean-session] [--ca-file CAFILE] [--ca-path CAPATH] [--ca-data CADATA] [ --will-topic WILL_TOPIC [--will-message WILL_MESSAGE] [--will-qos WILL_QOS] [--will-retain] ] [--extra-headers HEADER] [-r]

Options:
    -h --help           Show this screen.
    --version           Show version.
    --url BROKER_URL    Broker connection URL (musr conform to MQTT URI scheme (see https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme>)
    -c CONFIG_FILE      Broker configuration file (YAML format)
    -i CLIENT_ID        Id to use as client ID.
    -q | --qos QOS      Quality of service to use for the message, from 0, 1 and 2. Defaults to 0.
    -r                  Set retain flag on connect
    -t TOPIC            Message topic
    -m MESSAGE          Message data to send
    -f FILE             Read file by line and publish message for each line
    -s                  Read from stdin and publish message for each line
    -k KEEP_ALIVE       Keep alive timeout in second
    --clean-session     Clean session on connect (defaults to False)
    --ca-file CAFILE]   CA file
    --ca-path CAPATH]   CA Path
    --ca-data CADATA    CA data
    --will-topic WILL_TOPIC
    --will-message WILL_MESSAGE
    --will-qos WILL_QOS
    --will-retain
    --extra-headers EXTRA_HEADERS      JSON object with key-value pairs of additional headers for websocket connections
    -d                  Enable debug messages
    --whole-file FILE   Publish the file as a whole      
"""

import sys
from pathlib import Path  
sys.path.append(str(Path(__file__).parent.parent.parent))  # so python recognizes this directory as a module (Ivan)
import logging
import asyncio
import os
import json

import amqtt
from amqtt.client import MQTTClient, ConnectException
from docopt import docopt
from amqtt.utils import read_yaml_config

                                                                                    ### importing parser functions (Lisa)
import pandas as pd
import gpxpy
import gpxpy.gpx

file = '..\Hike-2022-06-30.gpx'                                                     ### relative file path (Lisa)

def gpxfile_to_dict(filename):
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
    print()
    return df.to_dict()
### Parser Stuff End

logger = logging.getLogger(__name__)


def _gen_client_id():
    import os
    import socket

    pid = os.getpid()
    hostname = socket.gethostname()
    return "amqtt_pub/%d-%s" % (pid, hostname)


def _get_qos(arguments):
    try:
        return int(arguments["--qos"][0])
    except:
        return None


def _get_extra_headers(arguments):
    try:
        return json.loads(arguments["--extra-headers"])
    except:
        return {}


def _get_message(arguments):
    if arguments["-n"]:
        yield b""
    if arguments["-m"]:
        test = gpxfile_to_dict(file)
        i = len(test)
        time = test["time"]
        lat = test["latitude"]
        long = test["longitude"]
        elev = test ["elevation"]
        test_2 = str(test)
        j = 0
        # while j < 3 :
        #     time_str = str(time[j])
        #     lat_str = str(lat[j])
        #     yield (time_str + " " + lat_str).encode(encoding="utf-8")
        #     j =+ 1
        print("in get_message_")
        yield arguments["-m"].encode(encoding="utf-8")
    if arguments["-f"]:
        try:
            with open(arguments["-f"]) as f:
                for line in f:
                    yield line.encode(encoding="utf-8")
        except:
            logger.error("Failed to read file '%s'" % arguments["-f"])
    if arguments["-l"]:
        import sys

        for line in sys.stdin:
            if line:
                yield line.encode(encoding="utf-8")
    if arguments["-s"]:
        import sys

        message = bytearray()
        for line in sys.stdin:
            message.extend(line.encode(encoding="utf-8"))
        yield message
    if arguments["--whole-file"]: 
        try:
            with open(arguments["--whole-file"]) as f:
                yield f.read().encode(encoding="utf-8")
        except:
            logger.error("Failed to read the whole file '%s'" % arguments["--whole-file"])


async def do_pub(client, arguments):
    running_tasks = []
    ## loop for 
    i = 0
    gps_file = gpxfile_to_dict(file)
    p = len(gps_file)
    time = gps_file["time"]
    lat = gps_file["latitude"]
    long = gps_file["longitude"]
    elev = gps_file["elevation"]
    while i < 3:                                                                ##### use p for entire file transmission (Lisa), looping publishing
        i += 1
        try:
            logger.info("%s Connecting to broker" % client.client_id)

            await client.connect(
                uri=arguments["--url"],
                cleansession=arguments["--clean-session"],
                cafile=arguments["--ca-file"],
                capath=arguments["--ca-path"],
                cadata=arguments["--ca-data"],
                extra_headers=_get_extra_headers(arguments),
            )
            qos = _get_qos(arguments)
            topic = arguments["-t"]
            retain = arguments["-r"]
            #####################
            
            str_time = str(time[i])                                             # converting into string
            str_lat = str(lat[i])
            str_long = str(long[i])
            str_elev = str(elev[i])
            
            
            arguments["-m"] = (str_time + " " + str_lat + " " + str_long + " " + str_elev) # fuse strings together into one, preparing for _get_message_ func
            #####################
            for message in _get_message(arguments):
                logger.info("%s Publishing to '%s'" % (client.client_id, topic))
                task = asyncio.ensure_future(client.publish(topic, message, qos, retain))
                running_tasks.append(task)
            if running_tasks:
                await asyncio.wait(running_tasks)
            await client.disconnect()
            logger.info("%s Disconnected from broker" % client.client_id)
        except KeyboardInterrupt:
            await client.disconnect()
            logger.info("%s Disconnected from broker" % client.client_id)
        except ConnectException as ce:
            logger.fatal("connection to '%s' failed: %r" % (arguments["--url"], ce))
        except asyncio.CancelledError:
            logger.fatal("Publish canceled due to previous error")


def main(*args, **kwargs):
    
    ###
    test = gpxfile_to_dict(file)

    
    ###
    if sys.version_info[:2] < (3, 7):
        logger.fatal("Error: Python 3.7+ is required")
        sys.exit(-1)

    arguments = docopt(__doc__, version=amqtt.__version__)
    # print(arguments)
    formatter = "[%(asctime)s] :: %(levelname)s - %(message)s"

    if arguments["-d"]:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format=formatter)

    if arguments["-c"]:
        config = read_yaml_config(arguments["-c"])
    else:
        config = read_yaml_config(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "default_client.yaml"
            )
        )
        logger.debug("Using default configuration")
    loop = asyncio.get_event_loop()

    client_id = arguments.get("-i", None)
    if not client_id:
        client_id = _gen_client_id()

    if arguments["-k"]:
        config["keep_alive"] = int(arguments["-k"])

    if (
        arguments["--will-topic"]
        and arguments["--will-message"]
        and arguments["--will-qos"]
    ):
        config["will"] = dict()
        config["will"]["topic"] = arguments["--will-topic"]
        config["will"]["message"] = arguments["--will-message"].encode("utf-8")
        config["will"]["qos"] = int(arguments["--will-qos"])
        config["will"]["retain"] = arguments["--will-retain"]

    client = MQTTClient(client_id=client_id, config=config, loop=loop)
    loop.run_until_complete(do_pub(client, arguments))
    loop.close()


if __name__ == "__main__":
    #print(test["time"])
    main()
