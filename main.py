"""
Avro example to serialize and deserialize user data
"""
import json
from io import BytesIO
from typing import Any, AnyStr, Dict, List

import avro
import avro.io
from avro.io import BinaryEncoder, BinaryDecoder, DatumWriter, DatumReader
from avro.schema import Schema


def open_user_data() -> List[dict]:
    with open(file="resources/user.json", mode="r") as file:
        return json.load(fp=file)


def get_schema_object() -> Schema:
    with open(file="resources/user.avsc", mode="r") as file:
        schema_data: AnyStr = file.read()

    return avro.schema.parse(json_string=schema_data)


def avro_serialize(user_data: dict, avro_schema: Schema) -> bytes:
    bytes_writer: BytesIO = BytesIO()
    encoder: BinaryEncoder = BinaryEncoder(writer=bytes_writer)
    writer: DatumWriter = DatumWriter(writers_schema=avro_schema)
    writer.write(datum=user_data, encoder=encoder)
    return bytes_writer.getvalue()


def avro_deserialize(raw_bytes: bytes, avro_schema: Schema) -> Dict[str, Any]:
    bytes_reader: BytesIO = BytesIO(initial_bytes=raw_bytes)
    decoder: BinaryDecoder = BinaryDecoder(reader=bytes_reader)
    reader: DatumReader = DatumReader(writers_schema=avro_schema)
    datum: object = reader.read(decoder=decoder)
    return json.loads(s=json.dumps(obj=datum))


def main() -> None:
    serialize_data: List[bytes] = list()

    actual_schema: Schema = get_schema_object()
    print(f"Avro schema: {actual_schema}")

    actual_data: List[dict] = open_user_data()
    print(f"User data: {actual_data}")

    for entry in actual_data:
        serialize_data.append(avro_serialize(user_data=entry, avro_schema=actual_schema))

    print(f"Serialized: {serialize_data}")

    for entry in serialize_data:
        deserialize_data: dict = avro_deserialize(raw_bytes=entry, avro_schema=actual_schema)
        print(f"Deserialize: {deserialize_data}")


if __name__ == '__main__':
    main()
