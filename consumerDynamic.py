import argparse
import requests
import base64

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from google.protobuf import descriptor_pb2
from google.protobuf import message_factory
from google.protobuf import text_format



def main(args):

    schema_id = 1  
    url = f"{args.schema_registry}/schemas/ids/{schema_id}"
    response = requests.get(url)
    schema_json = response.json()

    protobuf_schema_text = schema_json['schema']
    
    file_descriptor_set = descriptor_pb2.FileDescriptorSet()
    
    try:
        text_format.Merge(protobuf_schema_text, file_descriptor_set)
    except text_format.ParseError as e:
        print(f"Error parsing schema: {e}")
    
    factory = message_factory.MessageFactory()
    
    descriptor = file_descriptor_set.file[0].message_type[0]
    message_type = factory.GetPrototype(descriptor)

    topic = args.topic

    schema_registry_conf = {
    'schema.registry.url': args.schema_registry,
    'use.deprecated.format': False  
    }


    protobuf_deserializer = ProtobufDeserializer(message_type, schema_registry_conf)

    kafka_consumer_conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': 'protobuf-consumer-group',
        'auto.offset.reset': 'earliest',  
        'enable.auto.commit': False,
    }

    protobufConf = {'use.deprecated.format': False}

    print('is it? {}'.format('use.deprecated.format' not in protobufConf))

    consumer = Consumer(kafka_consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            customer = protobuf_deserializer(msg.value(), SerializationContext(topic, MessageField.VALUE))

            if customer is not None:
                print("Customer record {}:\n"
                      "\tcustomer_id: {}\n"
                      "\tfirst_name: {}\n"
                      "\tlast_name: {}\n"
                      "\tcreation_date: {}\n"
                      .format(msg.key(), customer.customer_id,
                              customer.first_name, customer.last_name,
                              customer.creation_date.ToJsonString()))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ProtobufDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_protobuf",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_protobuf",
                        help="Consumer group")

    main(parser.parse_args())