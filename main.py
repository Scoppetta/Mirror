import os
from datetime import datetime
from typing import Dict, Optional

import pymongo
from pymongo.collection import ReturnDocument
from pymongo import MongoClient
from pymongo.errors import AutoReconnect
from pymongo.operations import UpdateOne
from tenacity import retry_if_exception_type, stop_after_attempt, wait_random
from typing import Dict, List

from kytos.core import log
from kytos.core.db import Mongo
from kytos.core.retry import before_sleep, for_all_methods, retries
from napps.kytos.mef_eline.db.models import EVCBaseDoc, EVCUpdateDoc


@for_all_methods(
    retries,
    stop=stop_after_attempt(
        int(os.environ.get("MONGO_AUTO_RETRY_STOP_AFTER_ATTEMPT", "3"))
    ),
    wait=wait_random(
        min=int(os.environ.get("MONGO_AUTO_RETRY_WAIT_RANDOM_MIN", "1")),
        max=int(os.environ.get("MONGO_AUTO_RETRY_WAIT_RANDOM_MAX", "1")),
    ),
    before_sleep=before_sleep,
    retry=retry_if_exception_type((AutoReconnect,)),
)

class mirror:

    def __init__(self, get_mongo=lambda: Mongo()) -> None:
        self.mongo = get_mongo()
        self.db_client = self.mongo.client
        self.db = self.db_client[self.mongo.db_name]
        self.collection_name = "mirror_report" #MUDAR

    def insert_document(self, document: Dict) -> Dict:
        collection = self.db.get_collection(self.collection_name)
        collection.insert_one(document)
        return document

    def insert_list_of_documents(self, list_of_documents: List[Dict]) -> List[Dict]:
        collection = self.db.get_collection(self.collection_name)
        collection.insert_many(list_of_documents)
        return list_of_documents

    # Exemplo de uso
    #document = {
    #    "title": "Sample Report",
    #    "content": "This is the content of the sample report.",
    #    "created_at": "2024-06-10"
    #}
    #mirror_instance.insert_document(document)
    #
    #documents = [
    #    {"title": "Report 1", "content": "Content 1", "created_at": "2024-06-10"},
    #    {"title": "Report 2", "content": "Content 2", "created_at": "2024-06-10"}
    #]
    #mirror_instance.insert_list_of_documents(documents)


















#utilidade?
    def get_mirror(self, archived: Optional[bool] = False,
                     metadata: dict = None) -> Dict:
        """Get all mirrordata from database."""
        aggregation = []
        options = {"null": None, "true": True, "false": False}
        match_filters = {"$match": {}}
        aggregation.append(match_filters)
        if archived is not None:
            archived = options.get(archived, False)
            match_filters["$match"]["archived"] = archived
        if metadata:
            for key in metadata:
                if "metadata." in key[:9]:
                    try:
                        match_filters["$match"][key] = int(metadata[key])
                    except ValueError:
                        item = metadata[key]
                        item = options.get(item.lower(), item)
                        match_filters["$match"][key] = item
        aggregation.extend([
                {"$sort": {"_id": 1}},#perguntar o que mudar aqui
                {"$project": EVCBaseDoc.projection()},
            ]
        )
        mirrors = self.db.evcs.aggregate(aggregation)
        return {"circuits": {value["id"]: value for value in circuits}} #ajustar com as mudancas em id

    #a mudanca em cima vai afetar aqui tambem
    def get_circuit(self, circuit_id: str) -> Optional[Dict]:
        """Get a circuit."""
        return self.db.evcs.find_one({"_id": circuit_id},
                                     EVCBaseDoc.projection())



    #inutil

    def upsert_evc(self, evc: Dict) -> Optional[Dict]:
        """Update or insert an EVC"""
        utc_now = datetime.utcnow()
        model = EVCBaseDoc(
            **{
                **evc,
                **{"_id": evc["id"]}
            }
        ).model_dump(exclude={"inserted_at"}, exclude_none=True)
        model.setdefault("queue_id", None)
        updated = self.db.evcs.find_one_and_update(
            {"_id": evc["id"]},
            {
                "$set": model,
                "$setOnInsert": {"inserted_at": utc_now},
            },
            return_document=ReturnDocument.AFTER,
            upsert=True,
        )
        return updated

    def update_evc(self, evc: Dict) -> Optional[Dict]:
        """Update an EVC.
        This is needed to correctly set None values to fields"""

        # Check for errors in fields only.
        EVCUpdateDoc(
            **{
                **evc,
                **{"_id": evc["id"]}
            }
        )
        updated = self.db.evcs.find_one_and_update(
            {"_id": evc["id"]},
            {
                "$set": evc,
            },
            return_document=ReturnDocument.AFTER,
        )
        return updated

    def update_evcs(self, evcs: list[dict]) -> int:
        """Update EVCs and return the number of modified documents."""
        if not evcs:
            return 0

        ops = []
        utc_now = datetime.utcnow()

        for evc in evcs:
            evc["updated_at"] = utc_now
            model = EVCBaseDoc(
                **{
                    **evc,
                    **{"_id": evc["id"]}
                }
            ).model_dump(exclude_none=True)
            ops.append(
                UpdateOne(
                    {"_id": evc["id"]},
                    {
                        "$set": model,
                        "$setOnInsert": {"inserted_at": utc_now}
                    },
                )
            )
        return self.db.evcs.bulk_write(ops).modified_count

    def update_evcs_metadata(
        self, circuit_ids: list, metadata: dict, action: str
    ):
        """Bulk update EVCs metadata."""
        utc_now = datetime.utcnow()
        metadata = {f"metadata.{k}": v for k, v in metadata.items()}
        if action == "add":
            payload = {"$set": metadata}
        elif action == "del":
            payload = {"$unset": metadata}
        ops = []
        for _id in circuit_ids:
            ops.append(
                UpdateOne(
                    {"_id": _id},
                    {
                        **payload,
                        "$setOnInsert": {"inserted_at": utc_now}
                    },
                    upsert=False,
                )
            )
        return self.db.evcs.bulk_write(ops).modified_count






    #util