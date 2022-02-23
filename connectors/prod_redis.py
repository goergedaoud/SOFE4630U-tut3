from kafka import KafkaProducer;
import json;
import time
import io;
from avro.io import DatumWriter, BinaryEncoder
import avro.schema
import numpy as np;

bootstrap_servers='';
sasl_plain_username='';
sasl_plain_password='';
schemaID=0;

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
    
with open("ontarioTech.jpg", "rb") as f:
    value = f.read();
        
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,security_protocol='SASL_SSL',sasl_mechanism='PLAIN',\
    sasl_plain_username=sasl_plain_username,sasl_plain_password=sasl_plain_password,key_serializer=lambda v: v.encode())
producer.send('Images', value,key='OntarioTech');
producer.close();
