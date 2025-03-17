import requests
from utils.download_files import loadToDataFrames
import pandas as pd
from io import StringIO
import logging
# Toronto Open Data is stored in a CKAN instance. It's APIs are documented here:
# https://docs.ckan.org/en/latest/api/


def fetch_traffic_signal_locations_data() -> dict:

    # To hit our API, you'll be making requests to:
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

    # Datasets are called "packages". Each package can contain many "resources"
    # To retrieve the metadata for this package and its resources, use the package name in this page's URL:
    url = base_url + "/api/3/action/package_show"
    params = { "id": "traffic-signals-tabular"}
    package = requests.get(url, params = params).json()
    dataframes = []
    # To get resource data:
    for idx, resource in enumerate(package["result"]["resources"]):
        try:
            # for datastore_active resources:
            if resource["datastore_active"]:

                # To get all records in CSV format:
                url = base_url + "/datastore/dump/" + resource["id"]
                resource_dump_data = requests.get(url).text
                data = pd.read_csv(StringIO(resource_dump_data), sep=None, engine='python')
                dataframes.append(data)

            # To get metadata for non datastore_active resources:
            if not resource["datastore_active"]:
                url = base_url + "/api/3/action/resource_show?id=" + resource["id"]
                resource_metadata = requests.get(url).json()
                fileURL = resource_metadata['result']['url']
                fileExt = fileURL.split('.')[-1]
                data = loadToDataFrames(fileURL, fileExt)
                dataframes.append(data)
                # print(data)
        except Exception as e:
            logging.error(f"Error loading data from {url}: {e}")

        
        # From here, you can use the "url" attribute to download this file
    return dataframes


if __name__ == "__main__":
    fetch_traffic_signal_locations_data()
