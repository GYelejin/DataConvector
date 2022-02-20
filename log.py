import paho.mqtt.client as mqtt #import the client1
from influxdb import InfluxDBClient
from ast import literal_eval
import json
import logging
import xml.etree.ElementTree as ET

with open("appsettings.json", "r") as read_file:
    config = json.load(read_file)

logging.basicConfig(filename="app2.log", level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
logging.info("Program started")


def main():
    client.loop_forever()



def get_topic():
    topics = []
    for device in config["ProgramConfiguration"]["Devices"]:
        if device["DeviceDescription"]["DeviceType"] != "Correct":
            topics.append((wronn_topic(device["DeviceDescription"]["DeviceType"], device["DeviceDescription"]["Identifier"]), 0))    
    return topics

def get_normal_topic():
    topics = {}
    for device in config["ProgramConfiguration"]["Devices"]:
        if device["DeviceDescription"]["DeviceType"] != "Correct":
            topics[device["DeviceDescription"]["Identifier"]] = ("/".join([config["MqttConfiguration"]["MqttHomeDeviceTopic"], config["ProgramConfiguration"]["ServiceName"], "HassAnalogSensor" if device["DeviceDescription"]["DeviceType"] != "Plug" else "HassBinarySensor", device["DeviceDescription"]["Identifier"]]), 0)
    return topics



def gen_config_for_HA(Id):
    device = get_device_description_by_id[Id]
    return {"state_topic":f"+/+/Hass{'HassAnalogSensor' if device['DeviceDescription']['DeviceType'] != 'Plug' else 'HassBinarySensor'}Sensor/{Id}",
"name":f"{device['Name']}-{config['ProgramConfiguration']['TypesAliaes'][device['DataType']]}",
"unique_id":f"{Id}-{device['Name']}-{config['ProgramConfiguration']['TypesAliaes'][device['DataType']]}",
"device_class":f"{device['DeviceType'].lower()}",
"value_template":"{{" + f"value_json.{config['ProgramConfiguration']['TypesAliaes'][device['DataType']]} | is_defined" + "}}",
"unit_of_measurement":unit_of_measurement[device['DataType']],
"device":{
    "connections":[],
    "identifiers":[Id],
    "manufacturer":device['Manufacturer'],
    "model":device['Model'],
    "name":device['Name']}
    }



def get_device_description():
    res = {}
    for device in config["ProgramConfiguration"]["Devices"]:
        res[device['DeviceDescription']["Identifier"]] = device['DeviceDescription']
    return res

def get_device_types():
    data = []
    for device in config["ProgramConfiguration"]["Devices"]:
        data.append(device["DeviceDescription"]["DeviceType"])
    return data

def get_device_data():
    data = []
    for device in config["ProgramConfiguration"]["Devices"]:
        data.append(device["DeviceDescription"]["DataFormat"])
    return data

def get_data_type_aliaes(sensor_id):
    return type_name[data_id_type[int(sensor_id)-1]]




def typeOfInvalidByID(Id):
    return int(datatype_by_id[Id][-1:])

def wronn_topic(dataformat,Id):
    if dataformat == "Invalid1": return f"Device{Id}"
    elif dataformat == "Invalid2" : return f"Binary-{Id}-Sensor"
    elif dataformat == "Invalid3": return f"XmlSensor_{Id}"
    elif dataformat == "Invalid4": return f"CSV-{Id}"  
    elif dataformat == "Invalid5" : return f"Sensor{Id}"

def convector(data, Id):
    Ivalid = typeOfInvalidByID(Id)
    if Ivalid == 1:
        return literal_eval(data)["value"]
    elif Ivalid == 2:
        return int(data, 16) if TypeOfValue == "Integer" else int(data, 16) / 100 
    elif Ivalid == 3:
        return ET.fromstring(data+"</sensor>")[0][0][1]
    elif Ivalid == 4:
        return data.split(";")[1]
    elif Ivalid == 5:
        return value



def wrong_devisa():
    Id = []
    for device in config["ProgramConfiguration"]["Devices"]:
        if device["DeviceDescription"]["DeviceType"] != "Correct":
            Id.append(device["DeviceDescription"]["Identifier"])
    return Id



client = mqtt.Client("Convector") #create new instance
client.connect(config["MqttConfiguration"]["MqttUri"]) #connect to broker


def on_message(client, userdata, message):
    data = literal_eval(message.payload.decode('utf8'))
    logging.info(f"Receive message with topic: {message.topic}\n {' '*28}Message payload:{data}")
    Id = IdByTopic[message.topic]
    device = get_device_description_by_id[Id]
    client.publish(normal_topic[Id], payload= "{\"Id\":\"" +Id +"\",\""+ config['ProgramConfiguration']['TypesAliaes'][device['DataType']]+"\":\"" + convector(data, Id)+ "}\"")

client.on_message=on_message



unit_of_measurement = {'Temperature': '°C', 'Voltage': 'V', 'PressureHpa': 'hPa', 'Current': 'A', 'FrequencyHz': 'Hz', 'Humidity': '%'}



data_id_type = get_device_types()
type_name = config["ProgramConfiguration"]["TypesAliaes"]
datatype_by_id = get_device_data()
normal_topic = get_normal_topic()
get_device_description_by_id = get_device_description()

print(get_topic())

if __name__ == "__main__":
    client.subscribe(get_topic())
    for i in wrong_devisa():
        client.publish(i, payload=gen_config_for_HA(get_normal_topic()[Id]))
    main()


 

