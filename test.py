from pymodbus.client.sync import ModbusSerialClient
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.payload import BinaryPayloadDecoder 
from pymodbus.constants import Endian

client= ModbusSerialClient(method = "rtu", port="/dev/ttyUSB0",stopbits = 1, bytesize = 8, parity = 'N', baudrate= 9600)
client.connect()
result_ = client.read_holding_registers(address=40001,count=10, unit=1)
print(result_)

