from kafka import KafkaProducer;
import json;
import time
import io;
from avro.io import DatumWriter, BinaryEncoder
import avro.schema

schemaID=0;

bootstrap_servers='';
sasl_plain_username='';
sasl_plain_password='';

schema = avro.schema.parse(open("./schema.avsc").read())
writer = DatumWriter(schema)

def decode(msg_value):
    message_bytes = io.BytesIO(msg_value)
    message_bytes.seek(5)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict

def encode(value):
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(value, encoder)
    return schemaID.to_bytes(5, 'big')+bytes_writer.getvalue()

value={'id':5,'name':"user",'email':'test@gmail.com','department':"IT",'modified':int(1000*time.time())};
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,security_protocol='SASL_SSL',sasl_mechanism='PLAIN',\
    sasl_plain_username=sasl_plain_username,sasl_plain_password=sasl_plain_password,value_serializer=lambda m: encode(m))
producer.send('ToMySQL', value);
producer.close();
