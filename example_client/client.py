import logging
from example_client.base_client import BaseClient
from utils.workers import format_failed
import requests
from itertools import product

logging.getLogger("urllib3").setLevel(logging.DEBUG)
logging.getLogger("requests").setLevel(logging.DEBUG)

logger = logging.getLogger("retry_logger")


def get_data_for_params(imdb_id, base_variables, config=None, session=None, enum_params=None):
    """
    API Requests with pagination and variables. If enum_params, merges with base_variables.
    """
    headers = {'content-type': 'application/json'}
    url = "https://caching.graphql.imdb.com/"
    extensions = {
        "persistedQuery": {
            "sha256Hash": config["query_hash"],
            "version": 1,
        }
    }

    variables = base_variables.copy()
    if enum_params:
        variables.update(enum_params)

    all_data = []
    next_cursor = None
    has_next_page = False
    endpoint_name = config['name']

    while True:
        current_vars = variables.copy()
        if next_cursor and has_next_page:
            current_vars["after"] = next_cursor

        payload = {
            "operationName": config["endpoint_name"],
            "variables": current_vars,
            "extensions": extensions,
        }

        try:
            response = session.post(url, headers=headers, json=payload, timeout=(4, 3))
            response.raise_for_status()
            if response.history:
                logger.warning(f"Permanent Redirect : {imdb_id}")
                format_failed(imdb_id, "308 Permanent Redirect", str(response.url), config["table_name"])
                return None

            data = response.json()
            if enum_params:
                try:
                    for edge in BaseClient.safe_get(data, *config['data_location']):
                        edge["node"]["enum_params"] = enum_params.copy()
                except KeyError as k:
                    logger.warning(f"Unable to add enum: {k}")

            all_data.append(data)

            page_info = BaseClient.safe_get(data, *config['pageinfo_location'])
            if not page_info:
                break
            next_cursor = page_info.get("endCursor", None)
            has_next_page = page_info.get("hasNextPage", None)

            if not next_cursor or not has_next_page:
                break
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"json decode error for {imdb_id}: {e}")
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logger.warning(f"404 Not Found: {imdb_id}")
                format_failed(imdb_id, "404 Not Found", str(e), config['table_name'])
            else:
                logger.error(f"HTTP error for {imdb_id}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None

    return all_data


def enum_requests(imdb_id, record_count=50, config=None, session=None):
    """
    Checks for enums in the config. If present, iterate over all enum combinations.
    Otherwise, call the data function directly.
    """
    base_variables = {
        "first": config.get("page_size_min", min(50, record_count)),
        "locale": "en-US",
    }
    id_key = config.get("id_param", "const")
    base_variables[id_key] = imdb_id

    if config.get("sort"):
        base_variables["sort"] = config["sort"]

    if config.get("additional_vars"):
        base_variables.update(config["additional_vars"])

    enums = config.get("enums")
    all_results = []
    if enums:
        enum_keys = list(enums.keys())
        enum_values = list(enums.values())
        for combo in product(*enum_values):
            enum_params = dict(zip(enum_keys, combo))
            data = get_data_for_params(imdb_id, base_variables, config, session, enum_params)
            if data:
                all_results.extend(data)
    else:
        all_results = get_data_for_params(imdb_id, base_variables, config, session)
    return all_results


def get_location_details(imdb_id, record_count=100, config=None, session=None):
    """Get locations of titles."""
    headers = {'content-type': 'application/json'}
    url = "https://caching.graphql.imdb.com/"

    try:
        payload = {
            "operationName": config["endpoint_name"],
            "variables": {
                "const": imdb_id,
                "first": record_count,
                "locale": "en-US"
            },
            "extensions": {
                "persistedQuery": {
                    "sha256Hash": config["query_hash"],
                    "version": 1
                }
            }
        }

        response = session.post(url, headers=headers, json=payload, timeout=(3, 3))
        response.raise_for_status()

        if response.history:
            logger.warning(f"Permanent Redirect : {imdb_id}")
            format_failed(imdb_id, "308 Permanent Redirect", str(response.url), config["table_name"])
            return None

    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            logger.warning(f"404 Not Found: {imdb_id}")
            format_failed(imdb_id, "404 Not Found", str(e), config['table_name'])
        else:
            logger.error(f"HTTP error for {imdb_id}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None

    return response.json()
