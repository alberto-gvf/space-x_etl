import logging

import requests
import json
from utils import parse_arguments, PATH_RESOURCES_RAW

API_HEADER = "https://api.spacexdata.com/v5/launches/query"


def call_api(query_body):
    try:
        response = requests.post(API_HEADER, json=query_body)
        if response.status_code == 200:
            print("Call successfull. Returning response")
            return response.json()
        else:
            logging.error(f"Failed to fetch data. Status code: {response.status_code}")
            logging.error(f"Error message: {response.content}")
            return None
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

def build_query(page, start_date, end_date):
    return {
        "query": {
            "date_utc": {
                "$gte": f"{start_date}T00:00:00.000Z",
                "$lte": f"{end_date}T00:00:00.000Z"
            },
        },
        "options": {
            "select": {
                "static_fire_date_utc": 1,
                "static_fire_date_unix": 1,
                "rocket": 1,
                "success": 1,
                "failures": 1,
                "details": 1,
                "crew": 1,
                "ships": 1,
                "capsules": 1,
                "payloads": 1,
                "flight_number": 1,
                "name": 1,
                "date_utc": 1,
                "date_unix": 1,
                "date_local": 1,
                "date_precision": 1,
                "upcoming": 1,
                "cores": 1,
                "id": 1,
            },
            "sort": {"date_utc": "desc"},
            "pagination": True,
            "page": page,
            "limit": 2
        },
    }


def build_result(start_date, end_date):
    has_next_page = True
    page = 0
    result = []
    while has_next_page:
        if page == 0:
            response = call_api(build_query(1, start_date, end_date))
        else:
            response = call_api(build_query(response.get('nextPage'), start_date, end_date))
        result = result + response.get('docs')
        has_next_page = response.get('hasNextPage', False)
        page = response.get('nextPage', False)
    return result


def save(results, filename):
    json_str = json.dumps(results, indent=4)  # indent for pretty formatting, optional

    # Write JSON string to file
    with open(filename, 'w') as file:
        file.write(json_str)



def main():
    start_date, end_date = parse_arguments()

    print(f"Running loader for start_date:{start_date} and end_date: {end_date}")
    print(f"Running extraction for start_date:{start_date} and end_date: {end_date}")

    result = build_result(start_date, end_date)
    save(result, f"{PATH_RESOURCES_RAW}/data.json")


if __name__ == '__main__':
    main()