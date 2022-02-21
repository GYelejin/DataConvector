import paho.mqtt.client as mqtt  # import the client1
from ast import literal_eval
import json
import logging
import xml.etree.ElementTree as ET

with open("appsettings.json", "r") as read_file:
    config = json.load(read_file)

logging.basicConfig(filename="app2.log", level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')
logging.info("Program started")

with open("appsettings.json", "r") as read_file:
    config = json.load(read_file)


def main():
    logging.info(f"Subscribed to Topics: {[devices[i].InvalidTopic[0] for i in devices.keys()]}")
    client.subscribe([devices[i].InvalidTopic for i in devices.keys()])
    for sensor in devices.values():
        client.publish(sensor.config_topic(), payload=sensor.ConfigMsg)
    client.loop_forever()


class InvalidDevice:
    def __init__(self, Id, DeviceType, DataFormat, ValueType, Information):
        self.Id = Id
        self.DeviceType = DeviceType
        self.DataFormat = DataFormat
        self.Information = Information
        self.ValueType = ValueType
        self.generate_data()
        

    def invalid_topic(self):
        if self.DataFormat == "Invalid1":
            return f"Device{self.Id}"
        elif self.DataFormat == "Invalid2":
            return f"Binary-{self.Id}-Sensor"
        elif self.DataFormat == "Invalid3":
            return f"XmlSensor_{self.Id}"
        elif self.DataFormat == "Invalid4":
            return f"CSV-{self.Id}"
        elif self.DataFormat == "Invalid5":
            return f"Sensor{self.Id}"

    def chooseConvector(self):
        def Invalid2(data):
            convectors = {
                "Double": lambda data: str(int(data, 16) / 100),
                "Integer": lambda data: str(int(data, 16)),
                "Binary": lambda data:  "On" if int(data, 16) == 0 else "Off"}
            return convectors[self.ValueType]
        convectors = {
            "Invalid1": lambda data: literal_eval(data)["value"],
            "Invalid2": lambda data: Invalid2(data)(data),
            "Invalid3": lambda data: ET.fromstring(data+"</sensor>")[0][1].text,
            "Invalid4": lambda data: data.split(";")[1],
            "Invalid5": lambda data: data}
        return convectors[self.DataFormat]

    def get_valid_value(self, value):
        return self.Convector(value)

    def valid_topic(self):
        return "/".join([self.Information["MqttHomeDeviceTopic"],
                          self.Information["ServiceName"],
                          "HassAnalogSensor" if self.DeviceType != "Plug" else "HassBinarySensor", self.Id])

    def config_msg(self):
        if self.DeviceType == "Plug":
            return json.dumps({
                "state_topic": f"+/+/HassBinarySensor/{self.Id}", 
                "name": f"{self.Information['Name']}-{self.Information['DeviceTypeAlias']}",
                "unique_id": f"{self.Id}-{self.Information['Name']}-{self.Information['DeviceTypeAlias']}", 
                "device_class": "plug",
                "payload_on": "On",
                "payload_off": "Off",
                "value_template": "{ { value_json.state | is_defined } }", 
                "device": {
                    "connections": [],
                    "identifiers": [self.Id],
                    "manufacturer": self.Information['Manufacturer'],
                    "model": self.Information['Model'],
                    "name": self.Information['Name']}}).encode('utf8')
        else:
            return json.dumps({
                "state_topic": f"+/+/HassAnalogSensor/{self.Id}",
                "name": f"{self.Information['Name']}-{self.Information['DeviceTypeAlias']}",
                "unique_id": f"{self.Id}-{self.Information['Name']}-{self.Information['DeviceTypeAlias']}",
                "device_class": self.Information['DeviceClass'],
                "value_template": "{{" + f"value_json.{self.Information['DeviceTypeAlias']} | is_defined" + "}}",
                "unit_of_measurement": self.Information["Unit"],
                "device": {
                    "connections": [],
                    "identifiers": [self.Id],
                    "manufacturer": self.Information['Manufacturer'],
                    "model": self.Information['Model'],
                    "name": self.Information['Name']}}).encode('utf8')

    def config_topic(self):
        if self.DeviceType == "Plug":
            return "/".join(["homeassistant", "binary_sensor", "-".join([self.Id, self.Information["Name"], self.Information["DeviceTypeAlias"]]), "config"])
        else:
            return "/".join(["homeassistant", "sensor", "-".join([self.Id, self.Information["Name"], self.Information["DeviceTypeAlias"]]), "config"])

    def normal_post(self, value):
        logging.info(f"Convectored Value: {value} -> {self.Convector(value)}")
        logging.info("Sended Message: " + str({"Id": self.Id,"name" : self.Id, self.Information["DeviceTypeAlias"]: self.Convector(value)}))
        return json.dumps({"Id": self.Id,"name" : self.Id, self.Information["DeviceTypeAlias"]: self.Convector(value)})

    def generate_data(self):
        unit_of_measurement = {'Temperature': '°C', 'Voltage': 'V',
                               'PressureHpa': 'hPa', 'Current': 'A', 'FrequencyHz': 'Hz', 'Humidity': '%'}
        devicetypealias = {"Temperature": "temp", "Voltage": "volt", "PressureHpa": "pres",
                           "Current": "amps", "FrequencyHz": "freqh", "Humidity": "hum", "Plug": "state"}
        hass_device_class = {"Temperature": "temperature", "Voltage": "voltage", "PressureHpa": "pressure",
                           "Current": "current", "FrequencyHz": "frequency", "Humidity": "humidity", "Plug": "state"}
        self.Information["DeviceTypeAlias"] = devicetypealias[self.DeviceType]
        self.Information["Unit"] = unit_of_measurement[self.DeviceType]
        self.Information["DeviceClass"] = hass_device_class[self.DeviceType]
        self.InvalidTopic = (self.invalid_topic(), 0)
        self.ValidTopic = self.valid_topic()
        self.ConfigTopic = (self.config_topic(), 0)
        self.ConfigMsg = self.config_msg()
        self.Convector = self.chooseConvector()
        

def on_message(client, userdata, message):
    data = message.payload.decode('utf8')

    logging.info(f"Receive message with topic: {message.topic}\nMessage payload:{data}")

    client.publish(devices[message.topic].ValidTopic,
                   payload=devices[message.topic].normal_post(data))

client = mqtt.Client("Convector") #create new instance
client.connect(config["MqttConfiguration"]["MqttUri"]) #connect to broker

client.on_message = on_message


def all_invalid_devices():
    devices = {}
    for device in config["ProgramConfiguration"]["Devices"]:
        if device["DeviceDescription"]["DataFormat"] != "Correct":
            Information = {"Name": device["DeviceDescription"]["Name"],
                           "Model":  device["DeviceDescription"]["Model"],
                           "Manufacturer": device["DeviceDescription"]["Manufacturer"],
                           "MqttHomeDeviceTopic": config["MqttConfiguration"]["MqttHomeDeviceTopic"],
                           "ServiceName": config["ProgramConfiguration"]["ServiceName"]}
            sensor = InvalidDevice(device["DeviceDescription"]["Identifier"], 
            device["DeviceDescription"]["DeviceType"], 
            device["DeviceDescription"]["DataFormat"], 
            device["DeviceDescription"]["ValueType"], 
            Information)
            devices[sensor.InvalidTopic[0]] = sensor
    return devices


devices = all_invalid_devices()
if __name__ == "__main__":
    main()
