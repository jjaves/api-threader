import logging
from example_client.base_client import BaseClient
import datetime
import json
logger = logging.getLogger("retry_logger")


def parse_locations(data, item_id):
    """
    API-specific logic that transforms the raw JSON.
    We can reuse BaseClient.safe_get, BaseClient.extract_array, etc.
    """

    safe_get = BaseClient.safe_get
    extract_array = BaseClient.extract_array

    master_list = []
    if not data:
        logger.error("No data passed to parse_locations.")
        return None
    try:
        if isinstance(data, list):
            for obj in data:
                list_data = safe_get(obj, "data", "item", "locations", "edges")
                if list_data:
                    master_list.extend(list_data)
        else:
            master_item = safe_get(data, "data", "item", "locations", "edges")
            if master_item:
                master_list.append(master_item) 
                if isinstance(master_item, list):
                    master_list.extend(master_item)
                else:
                    master_list.append(master_item)


    except Exception as e:
        logger.error(f"Error processing initial data structure: {e}")
        return None

    if not master_list:
        logger.warning(f"No location edges found for item_id: {item_id}")
        return []

    try:
        locations_data = []
        for item_edge in master_list:
            node_data = safe_get(item_edge, "node")
            if not node_data:
                logger.warning(f"No node data in edge for item_id: {item_id}")
                continue

            raw_list = extract_array(
                safe_get(node_data, "displayableProperty", "qualifiersInMarkdownList"),
                [("markdown",)]
            )
            markdown_value_list = json.dumps(raw_list) if isinstance(raw_list, list) else raw_list

            item_details = {
                "item_id": item_id,
                "id": safe_get(node_data, "id"),
                "location": safe_get(node_data, "location"),
                "text": safe_get(node_data, "text"),
                "usersInterested": safe_get(node_data, "interestScore", "usersInterested"),
                "usersVoted": safe_get(node_data, "interestScore", "usersVoted"),
                "markdownValueList": markdown_value_list,
                "markdownValue": safe_get(node_data, "displayableProperty", "value", "markdown"),
                "recorded_at": datetime.datetime.utcnow().isoformat()
            }
            locations_data.append(item_details)
        return locations_data
    except Exception as e:
        logger.error(f"Error in parse_locations processing nodes: {e}")
        return None