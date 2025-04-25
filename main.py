"""
Avro example to serialize and deserialize user data
"""

import json
from io import BytesIO
from typing import Any

import avro
import avro.schema
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from avro.schema import Schema


def open_user_data() -> list[dict]:
    """Open user data file

    :return: User data object
    """
    with open(file="resources/user.json", mode="r") as file:
        return json.load(fp=file)


def get_schema_object() -> Schema:
    """Open Avro schema file

    :return: Avro schema content
    """
    with open(file="resources/user.avsc", mode="r") as file:
        schema_data: str = file.read()

    return avro.schema.parse(json_string=schema_data)


def avro_serialize(user_data: dict, avro_schema: Schema) -> bytes:
    """Avro serialize user data

    :param user_data: Given user data
    :param avro_schema: Avro schema content
    :return: Serialized bytes
    """
    bytes_writer: BytesIO = BytesIO()
    encoder: BinaryEncoder = BinaryEncoder(writer=bytes_writer)
    writer: DatumWriter = DatumWriter(writers_schema=avro_schema)
    writer.write(datum=user_data, encoder=encoder)
    return bytes_writer.getvalue()


def avro_deserialize(raw_bytes: bytes, avro_schema: Schema) -> dict[str, Any]:
    """Avro deserialize user data

    :param raw_bytes: Avro serialized bytes
    :param avro_schema: Avro schema content
    :return: Deserialize data
    """
    bytes_reader: BytesIO = BytesIO(initial_bytes=raw_bytes)
    decoder: BinaryDecoder = BinaryDecoder(reader=bytes_reader)
    reader: DatumReader = DatumReader(writers_schema=avro_schema)
    datum: object = reader.read(decoder=decoder)
    return json.loads(s=json.dumps(obj=datum))


def main() -> None:
    """Entry point"""
    serialize_data: list[bytes] = list()

    actual_schema: Schema = get_schema_object()
    print(f"Avro schema: {actual_schema}")

    actual_data: list[dict] = open_user_data()
    print(f"User data: {actual_data}")

    for entry in actual_data:
        serialize_data.append(avro_serialize(user_data=entry, avro_schema=actual_schema))

    print(f"Serialized: {serialize_data}")

    for entry in serialize_data:
        deserialize_data: dict[str, Any] = avro_deserialize(raw_bytes=entry, avro_schema=actual_schema)
        print(f"Deserialize: {deserialize_data}")


if __name__ == "__main__":
    main()
