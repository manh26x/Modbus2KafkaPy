import time
from datetime import datetime

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.client import ModbusTcpClient
import pandas as pd
from kafka import KafkaProducer
import json
import schedule


def get_param():
    data = pd.read_excel(r"CauHinhThongSoPaChien.xlsx")
    return data


def job():
    configModbus = json.load(open("CauHinhModbus.json"))
    client = ModbusTcpClient(configModbus['host'], configModbus['port'])

    params = get_param()
    values = []
    for index, row in params.iterrows():
        address = params.at[index, 'Addr']
        count = params.at[index, 'Rid']
        rr = client.read_holding_registers(address, count, slave=configModbus['slaveId'])
        assert not rr.isError()
        decoder = BinaryPayloadDecoder.fromRegisters(rr.registers, Endian.Big, Endian.Little)
        if params.at[index, 'DataType'] == 'bit_float_32':
            values.append(round(decoder.decode_32bit_float(), 4))
        elif params.at[index, 'DataType'] == 'bit_undecimal_16':
            values.append(decoder.decode_8bit_uint())
    params['Values'] = values

    kafkaConfig = json.load(open("CauHinhKafka.json"))
    producer = KafkaProducer(bootstrap_servers=kafkaConfig['bootstrapServers'], api_version=(0, 10, 0),
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    producer.send(kafkaConfig['topic'], params[['Name', 'Values']].to_json())
    dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print('gen data' + dt_string)


while True:
    job()
    now = datetime.now()
    if now.microsecond != 0:
        microsecond_sleep = 1.0 - now.microsecond / 1000000.0
        time.sleep(microsecond_sleep)
