import requests
import re
import argparse
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool
from google.protobuf import message_factory
from google.protobuf import timestamp_pb2
from confluent_kafka import Consumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField


def fetch_protobuf_schema(schema_registry_url,topic_name):
    url = f"{schema_registry_url}/subjects/{topic_name}-value/versions/latest"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()['schema']

def create_protobuf_message_type(schema_text):
    # Create a new FileDescriptorProto
    file_descriptor = descriptor_pb2.FileDescriptorProto()

    # Set the package and syntax directly from the schema text
    lines = schema_text.splitlines()

    # Extract syntax from the first line if present
    syntax_pattern = re.compile(r'syntax\s*=\s*"([^"]+)";')
    syntax_match = syntax_pattern.search(lines[0])
    if syntax_match:
        file_descriptor.syntax = syntax_match.group(1)

    # Set package name from the package declaration
    package_pattern = re.compile(r'package\s+([a-zA-Z0-9_.]+);\s*')
    for line in lines:
        line = line.strip()
        if line.startswith('package'):
            package_match = package_pattern.search(line)
            if package_match:
                file_descriptor.package = package_match.group(1)
                break  # Package found, exit the loop

    # Now parse the rest of the schema to extract messages and fields
    message_type = None
    for line in lines:
        line = line.strip()

        # Handle message declarations
        if line.startswith('message'):
            message_name = line.split(' ')[1].strip('{')
            message_type = file_descriptor.message_type.add()
            message_type.name = message_name

        # Handle end of message declarations
        elif line.startswith('}'):
            message_type = None  # End of the current message

        # Handle field definitions
        elif message_type and '=' in line:
            # Extract field definitions
            field_definition = line.split('=')[0].strip()  # Get the part before '='
            field_parts = field_definition.split()
            if len(field_parts) >= 2:  # Ensure there are at least two parts (type and name)
                field_type = field_parts[0]
                field_name = field_parts[1]

                # Attempt to extract field number
                number_part = line.split('=')[1].strip()
                if number_part.endswith(';'):
                    number_part = number_part[:-1]  # Remove the trailing ';'
                try:
                    field_number = int(number_part)
                except ValueError:
                    print(f"Warning: Unable to parse field number from line: {line}")
                    continue  # Skip this field if parsing fails

                # Add field to message type
                field_descriptor = message_type.field.add()
                field_descriptor.name = field_name
                field_descriptor.number = field_number

                # Basic type mapping (expand as necessary)
                if field_type == "int32":
                    field_descriptor.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT32
                elif field_type == "string":
                    field_descriptor.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
                elif field_type == "google.protobuf.Timestamp":
                    field_descriptor.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
                    field_descriptor.type_name = "google.protobuf.Timestamp"
                # Add more type mappings as necessary
            else:
                print(f"Warning: Unexpected field definition format: {line}")

    # Create a DescriptorPool
    pool = descriptor_pool.DescriptorPool()

    # Add the serialized timestamp descriptor
    try:
        timestamp_serialized = timestamp_pb2.DESCRIPTOR.serialized_pb
        pool.AddSerializedFile(timestamp_serialized)
    except Exception as e:
        print(f"Error adding serialized timestamp descriptor: {e}")

    # Serialize the file descriptor
    file_descriptor_proto = file_descriptor.SerializeToString()

    # Add the file descriptor to the pool
    try:
        pool.AddSerializedFile(file_descriptor_proto)
    except Exception as e:
        print(f"Error adding serialized file descriptor: {e}")

    # Find the dynamically created message class
    message_class = pool.FindMessageTypeByName(f"{file_descriptor.package}.{file_descriptor.message_type[0].name}")
    
    # Ensure we return the message class itself
    return message_factory.GetMessageClass(message_class)

def main(args):

    kafka_consumer_conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': args.group,
        'auto.offset.reset': 'earliest',
    }

    schema_registry_conf = {
        'schema.registry.url': args.schema_registry,
        'use.deprecated.format': False
    }
    consumer = Consumer(kafka_consumer_conf)
    consumer.subscribe([args.topic])
    schema_text = fetch_protobuf_schema(args.schema_registry, args.topic)
    message_class = create_protobuf_message_type(schema_text)
    protobuf_deserializer = ProtobufDeserializer(message_class, schema_registry_conf)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                print(f"Error: {msg.error()}")
                continue
            
            customer = protobuf_deserializer(
                msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
            )
            
            if customer is not None:
                print("Customer record {}:\n"
                      "\tcustomer_id: {}\n"
                      "\tfirst_name: {}\n"
                      "\tlast_name: {}\n"
                      "\tcreation_date: {}\n"
                      .format(msg.key(), customer.customer_id,
                              customer.first_name, customer.last_name,
                              customer.creation_date.ToJsonString()))
            
            consumer.commit()

    except KeyboardInterrupt:
        pass
    finally:
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