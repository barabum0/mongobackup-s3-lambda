import os
from typing import Any, Mapping
import json

import bson
from bson.json_util import dumps
from pymongo import MongoClient
from pymongo.collection import Collection


def get_config() -> dict[str, Any]:
    mongo__url = os.environ.get("MONGO__URL", None)
    mongo__db_name = os.environ.get("MONGO__DB_NAME", None)
    dump__dir = os.environ.get("DUMP__DIR", None)

    if not all(v is not None for v in (mongo__url, mongo__db_name, dump__dir)):
        print(
            json.dumps(
                {
                    "msg": "Not all of the required env vars were set.",
                    "level": "FATAL",
                    "stream_name": "main",
                }
            )
        )
        raise RuntimeError("Not all of the required env vars were set.")

    return {
        "mongo__url": mongo__url,
        "mongo__db_name": mongo__db_name,
        "dump__dir": dump__dir,
    }


def dump_collection(collection: Collection[Mapping[str, Any]], db_dir: str) -> None:
    bson_file_path = os.path.join(db_dir, f"{collection.name}.bson")
    metadata_file_path = os.path.join(db_dir, f"{collection.name}.metadata.json")

    with open(bson_file_path, "wb") as bson_file:
        for document in collection.find():
            bson_file.write(bson.BSON.encode(document))

    # Extract index information for the collection
    indexes = collection.list_indexes()
    index_list = []

    for index_info in indexes:
        # Convert index_info to a dict if it's a SON object
        index_info = dict(index_info)

        # Remove the 'ns' field (namespace) as it's not required
        index_info.pop("ns", None)

        # Append the index_info to the index_list
        index_list.append(index_info)

    # Retrieve the collection options
    options = collection.options()

    # Get the collection UUID if available
    collection_info = collection.database.command(
        "listCollections", filter={"name": collection.name}
    )
    collection_uuid = None
    if "cursor" in collection_info:
        for coll in collection_info["cursor"]["firstBatch"]:
            if (
                coll["name"] == collection.name
                and "info" in coll
                and "uuid" in coll["info"]
            ):
                collection_uuid = coll["info"]["uuid"]
                break

    # Create metadata document
    metadata = {
        "indexes": index_list,
        "uuid": collection_uuid,
        "collectionName": collection.name,
        "type": "collection",
        "options": options,
    }

    # Write metadata to JSON file using Extended JSON format
    with open(metadata_file_path, "w") as metadata_file:
        metadata_json = dumps(
            metadata, json_options=bson.json_util.CANONICAL_JSON_OPTIONS
        )
        metadata_file.write(metadata_json)


def dump_db(config: dict[str, Any]) -> None:
    print(
        json.dumps(
            {
                "msg": f"Dumping {config['mongo__db_name']}...",
                "level": "INFO",
                "stream_name": "main",
            }
        )
    )

    client: MongoClient = MongoClient(config["mongo__url"])
    db = client.get_database()

    db_dir = str(os.path.join(config["dump__dir"], config["mongo__db_name"]))
    os.makedirs(db_dir, exist_ok=True)

    collections = db.list_collection_names()

    for collection_name in collections:
        print(
            json.dumps(
                {
                    "msg": f"Dumping collection {collection_name}...",
                    "level": "INFO",
                    "stream_name": "main",
                }
            )
        )
        collection = db.get_collection(collection_name)
        dump_collection(collection, db_dir)
        print(
            json.dumps(
                {
                    "msg": f"Successfully dumped collection {collection_name}!",
                    "level": "INFO",
                    "stream_name": "main",
                }
            )
        )

    print(
        json.dumps(
            {
                "msg": f"Successfully dumped {config['mongo__db_name']}!",
                "level": "INFO",
                "stream_name": "main",
            }
        )
    )
