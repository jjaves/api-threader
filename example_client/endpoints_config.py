from example_client.endpoints.locations import parse_locations
from example_client.client import enum_requests

endpoints = {
    "locations": {
        "name": "locations",
        "table_name": "location_details",
        "failed_table": "failed_location_details_requests",
        "source_table": "db_location_for_ids",
        "filter_func": parse_locations,
        "get_function": enum_requests,
        "endpoint_name": "LocationsPaginated",
        "query_hash": "abc",
        "column_counter": None,
        "data_location": ("path", "to", "json", "data"),
        "pageinfo_location": ("path", "to", "json", "pageInfo"),
        "id_param": "unique_id",
    },
}