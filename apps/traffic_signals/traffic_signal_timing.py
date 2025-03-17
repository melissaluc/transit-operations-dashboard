# API Data Limitations
# These traffic signal phasing and interval sequences cannot be presented as evidence in a court of law. Requests for official timings must follow the process stipulated on the City's website.
# These traffic signal phasing and interval sequences cannot be presented as part of a development application. Requests for official timings must follow the process stipulated on the City's website.
# These traffic signal phasing and interval sequences are only available if there is constant communication between the central system and the field controllers. If there is a lapse in communication, the data can be incorrect or incomplete.
# Traffic signals on the City's two traffic adaptive systems (SCOOT and SCATS) are not available.
from utils.download_files import loadToDataFrames
from utils.logging_helper import log_dataframe_dict_keys_dimension

import requests
import logging

# Toronto Open Data is stored in a CKAN instance. It's APIs are documented here:
# https://docs.ckan.org/en/latest/api/

def fetch_traffic_signal_timing_data() -> dict:
    """
    Fetch traffic signal timing data from Toronto Open Data CKAN instance.
    """
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

    # Datasets are called "packages". Each package can contain many "resources"
    # To retrieve the metadata for this package and its resources, use the package name in this page's URL:
    url = base_url + "/api/3/action/package_show"
    params = {"id": "traffic-signal-timing"}
    package = requests.get(url, params=params).json()
    dataframes = []
    # To get resource data:
    for idx, resource in enumerate(package["result"]["resources"]):

        # To get metadata for non datastore_active resources:
        if not resource["datastore_active"]:
            url = base_url + "/api/3/action/resource_show?id=" + resource["id"]
            resource_metadata = requests.get(url).json()
            if resource_metadata['result']['url']:
                logging.info(f"Downloading traffic signal timing at {resource_metadata['result']['url']}")
                data = loadToDataFrames(resource_metadata['result']['url'], 'csv', ',')
                log_dataframe_dict_keys_dimension(data)
                logging.info(f"Returning data")
                dataframes.append(data)
            else:
                logging.info(f'No resource URL to download traffic signal data for {resource["id"]}')
    
    return dataframes




if __name__ == "__main__":
    fetch_traffic_signal_timing_data()



