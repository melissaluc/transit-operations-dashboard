from dotenv import load_dotenv
import os
import requests
import pandas as pd

import logging

from utils.download_files import loadToDataFrames


load_dotenv()
API_REFRESH_TOKEN = os.getenv('API_REFRESH_TOKEN')
GTFS_FEED_BASE_URL = os.getenv('GTFS_FEED_BASE_URL')
MOBILITY_DB_URL = os.getenv('MOBILITY_DB_URL')


#  Generate JWT Token

# Access GTFS data
## Loop through response to get ["source_info"]["producer_url"] or ["latest_dataset"]["hosted_url"] to get GTFS data date updated ["latest_dataset"][downloaded_at]


def getAuthToken():
    """
    Get JWT Token
    """
    headers =  {
        "Content-Type": 'application/json'
    }

    data = { 
    "refresh_token": API_REFRESH_TOKEN
    }

    url = MOBILITY_DB_URL
    resJwtToken = requests.post(url, headers=headers, json=data)

    try:
        resJwtTokenBody = resJwtToken.json()
        jwtToken = resJwtTokenBody.get("access_token")
        if jwtToken:
            return jwtToken
        else:
            logging.info("Invalid Token Format:", jwtToken)
    except ValueError:
        logging.error(resJwtToken.text)

def getGTFSFeedData(JWT_TOKEN: str, QUERY_PARAMS: dict | None =None) -> list:
    """
    Get GTFS feed data

    Example of QUERY_PARAMS
    QUERY_PARAMS: {
        "limit": 100,
        "offset": 0,
        "provider": "MiWay",
        "producer_url": "http://www.miway.ca",
        "entity_types": "vp,sa,tu",
        "country_code": "CA",
        "subdivision_name": "Ontario",
        "bounding_filter_method":"completely_enclosed",
        "municipality": "Mississauga",
        "is_official": False
    }
    """

    headers = {
        "Accept": "application/json",
        "Authorization": f'Bearer {JWT_TOKEN}'
    }
    url = GTFS_FEED_BASE_URL
    res = requests.get(url, params=QUERY_PARAMS, headers=headers)
    resBody = res.json()
    data = []
    for feed in resBody:
        feed_provider = feed["provider"]
        if(feed["latest_dataset"] is not None and feed['status'] == 'active'):
            data.append({
                "provider": feed_provider,
                "hosted_url": feed["latest_dataset"]["hosted_url"],
                "date": feed["latest_dataset"]["downloaded_at"]
            })
            logging.info(f'Appended feed from {feed_provider} from hosted url')
        elif(feed["source_info"]["producer_url"] and feed['status'] == 'active'):
            data.append({
                "provider": feed_provider,
                "hosted_url": feed["source_info"]["producer_url"],
                "date": feed["created_at"]
            })
            logging.info(f'Appended feed from {feed_provider} from source url')
        else:
            logging.info(f'No GTFS data available for {feed_provider}')
    return data





def main(
        AGENCY: str | None = None, 
        COUNTRY_CODE: str | None = None, 
        SUBDIVISION_NAME: str | None = None, 
        MUNICIPALITY: str | None = None
        ) -> dict | None:
    
    jwt_token = getAuthToken()
    if jwt_token:

        query_params = {
            "limit": 100,
            "offset": 0,
            "provider": AGENCY,
            "entity_types": "vp,sa,tu",
            "country_code": COUNTRY_CODE,
            "subdivision_name": SUBDIVISION_NAME,
            "bounding_filter_method":"completely_enclosed",
            "municipality": MUNICIPALITY,
            "is_official": False,
        }

        query_params = {k: v for k, v in query_params.items() if v is not None}

        # Get GTFS feed data download url
        gtfs_feed_data = getGTFSFeedData(jwt_token, query_params)

        # Download GTFS feed data via extracted urls in gtfs_feed_data
        agency_data = {}
        for feed in gtfs_feed_data:
            agency = feed['provider']
            feed_source_url = feed['hosted_url']
            feed_date= feed['date']
            logging.info(feed['date'])
            logging.info(f"Downloading {feed['provider']} feed from {feed_source_url} on {feed_date}")
            dataframes = loadToDataFrames(feed_source_url, 'txt', ',')
            agency_data.setdefault(agency, {})
            agency_data[agency]['data'] = dataframes
            agency_data[agency]['date'] = feed['date']
        return agency_data

if __name__ == "__main__":
    main()
