import os
import tarfile
import io
from datetime import datetime
from typing import Any

import bson
from pymongo import MongoClient
from bson.json_util import dumps  # Use dumps for Extended JSON
import json


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

    config = {
        "mongo__url": mongo__url,
        "mongo__db_name": mongo__db_name,
        "dump__dir": dump__dir,
    }

    print(
        json.dumps(
            {
                "msg": f"Configuration successfully retrieved. Database: '{mongo__db_name}', Dump directory: '{dump__dir}'.",
                "level": "INFO",
                "stream_name": "main",
            }
        )
    )

    return config


def dump_collection(collection_name: str, db, tar: tarfile.TarFile) -> None:
    """
    Dumps a single collection to the tarfile.

    Args:
        collection_name (str): Name of the collection to dump.
        db: The database object.
        tar (tarfile.TarFile): Tarfile object to add the collection dump to.
        config (dict): Configuration dictionary containing database info.
    """
    print(
        json.dumps(
            {
                "msg": f"Starting dump of collection '{collection_name}'.",
                "level": "INFO",
                "stream_name": "main",
            }
        )
    )

    bson_file_name = f"dump/{db.name}/{collection_name}.bson"
    metadata_file_name = f"dump/{db.name}/{collection_name}.metadata.json"

    collection = db[collection_name]

    try:
        # Dump collection data to a BytesIO object
        bson_buffer = io.BytesIO()
        cursor = collection.find()

        # Iterate over documents in the collection
        for document in cursor:
            # Encode each document to BSON and write to the buffer
            bson_buffer.write(bson.BSON.encode(document))

        # Create a TarInfo object for the bson file
        bson_tarinfo = tarfile.TarInfo(name=bson_file_name)
        bson_tarinfo.size = bson_buffer.tell()
        bson_buffer.seek(0)  # Reset buffer pointer to the beginning

        # Add the bson buffer to the tarfile
        tar.addfile(bson_tarinfo, fileobj=bson_buffer)

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
        collection_info = db.command(
            "listCollections", filter={"name": collection_name}
        )
        collection_uuid = None
        if "cursor" in collection_info:
            for coll in collection_info["cursor"]["firstBatch"]:
                if (
                    coll["name"] == collection_name
                    and "info" in coll
                    and "uuid" in coll["info"]
                ):
                    collection_uuid = coll["info"]["uuid"]
                    break

        # Create metadata document
        metadata = {
            "indexes": index_list,
            "uuid": collection_uuid,
            "collectionName": collection_name,
            "type": "collection",
            "options": options,
        }

        # Write metadata to a BytesIO object using Extended JSON format
        metadata_buffer = io.BytesIO()
        metadata_json = dumps(
            metadata, json_options=bson.json_util.CANONICAL_JSON_OPTIONS
        )
        metadata_buffer.write(metadata_json.encode("utf-8"))
        metadata_size = metadata_buffer.tell()
        metadata_buffer.seek(0)  # Reset buffer pointer to the beginning

        # Create a TarInfo object for the metadata file
        metadata_tarinfo = tarfile.TarInfo(name=metadata_file_name)
        metadata_tarinfo.size = metadata_size

        # Add the metadata buffer to the tarfile
        tar.addfile(metadata_tarinfo, fileobj=metadata_buffer)

        # Close the buffers
        bson_buffer.close()
        metadata_buffer.close()

        print(
            json.dumps(
                {
                    "msg": f"Successfully dumped collection '{collection_name}'.",
                    "level": "INFO",
                    "stream_name": "main",
                }
            )
        )
    except Exception as e:
        print(
            json.dumps(
                {
                    "msg": f"Error dumping collection '{collection_name}': {e}",
                    "level": "ERROR",
                    "stream_name": "main",
                }
            )
        )
        raise


def dump_database(config: dict[str, Any]) -> None:
    """
    Dumps the entire database to a tarfile.

    Args:
        config (dict): Configuration dictionary containing database info.
    """
    db_name = config["mongo__db_name"]
    mongo_url = config["mongo__url"]
    dump_dir = config["dump__dir"]
    client = MongoClient(mongo_url)
    db = client[db_name]

    print(
        json.dumps(
            {
                "msg": f"Starting database dump for database '{db_name}'.",
                "level": "INFO",
                "stream_name": "main",
            }
        )
    )

    try:
        # Output tarfile path
        tarfile_path = os.path.join(
            dump_dir,
            f"{db_name}_backup_{datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")}.tar.gz",
        )

        # Create the dump directory if it doesn't exist
        os.makedirs(dump_dir, exist_ok=True)

        # Create a tarfile object for writing
        with tarfile.open(tarfile_path, "w") as tar:
            # Get the list of collections
            collections = db.list_collection_names()

            # Iterate over all collections in the database
            for collection_name in collections:
                dump_collection(collection_name, db, tar)

        print(
            json.dumps(
                {
                    "msg": f"Successfully completed database dump for database '{db_name}'.",
                    "level": "INFO",
                    "stream_name": "main",
                }
            )
        )
    except Exception as e:
        print(
            json.dumps(
                {
                    "msg": f"Error during database dump for database '{db_name}': {e}",
                    "level": "ERROR",
                    "stream_name": "main",
                }
            )
        )
        raise


def handler(event: Any, context: Any) -> dict[str, Any]:
    try:
        dump_database(get_config())
        return {
            "statusCode": 200,
            "body": {"msg": "OK"},
        }
    except Exception as e:
        print(
            json.dumps(
                {
                    "msg": f"Unhandled exception in handler: {e}",
                    "level": "FATAL",
                    "stream_name": "main",
                }
            )
        )
        return {
            "statusCode": 500,
            "body": {"msg": "Internal Server Error"},
        }
