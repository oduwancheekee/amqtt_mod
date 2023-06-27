#!/bin/bash

# Command 1: Launch Broker Script
osascript -e 'tell application "Terminal" to do script "python /Users/neerajchauhan/amqtt_mod/amqtt/scripts/broker_script.py -c /Users/neerajchauhan/amqtt_mod/config1.yml"'
sleep 3
# Command 2: Launch Subscriber Script
osascript -e 'tell application "Terminal" to do script "python /Users/neerajchauhan/amqtt_mod/amqtt/scripts/sub_script.py --url mqtt://localhost:1883 -t the_topic/#"'
sleep 3
# Command 3: Launch Publisher Script
osascript -e 'tell application "Terminal" to do script "python /Users/neerajchauhan/amqtt_mod/amqtt/scripts/pub_script.py --url mqtt://localhost:1883 -t the_topic/unit1 -m \"Hello\""'
