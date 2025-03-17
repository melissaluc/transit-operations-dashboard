import logging
import zipfile 
import io
import requests
import pandas as pd
import geopandas as gpd



def handleFileExtension(fileExt, f):
    try:
        if fileExt in ['csv', 'txt']:
            return pd.read_csv(f, sep=None, engine='python')
        elif fileExt == 'geojson':
            return gpd.read_file(f)
        elif fileExt == 'gpkg':
            return gpd.read_file(f, driver='pyogrio', use_arrow=True)
        else:
            logging.warning(f"File format {fileExt} not supported.")
            return None
    except Exception as e:
        logging.error(f"Error processing {fileExt}: {e}")
        return None



def loadToDataFrames(URL: str, fileExt: str='csv') -> dict | None:
    """
    Download zip files and read individual files into dataframes returning a dict object containing dataframes or geodataframes
    \nSupports files
    """

    response = requests.get(URL)
    # print(response.content)
    if response.status_code == 200:
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                logging.info(f"Processing zip file retrieved from URL")
                dataframes = {}
                for file in zip_ref.namelist():
                    if any(file.endswith(ext) for ext in ['.csv', '.txt', '.geojson', '.gpkg']):
                        df_name = file.replace(f".{fileExt}", '')
                        with zip_ref.open(file) as f:
                            df = handleFileExtension(fileExt, f)
                            if df is None:
                                logging.warning(f"Skipping {file} due to being unable to process.")
                                continue  
                            dataframes[df_name] = df

                            logging.info(f"Loaded {file} into DataFrame")
                            logging.info(f"DataFrame {df_name} has {len(df)} rows")
                return dataframes
            
        except zipfile.BadZipFile:
            logging.info("The file retrieved from URL is not a ZIP. Processing as a single file.")
            fileExt = URL.split('.')[-1]  # Extract file extension from URL
            df_name = URL.split('/')[-1].replace(f".{fileExt}", '')
            with io.BytesIO(response.content) as f:
                df = handleFileExtension(fileExt, f)
                if df is not None:
                    logging.info(f"Loaded {URL} into DataFrame/GeoDataFrame")
                    return {df_name: df}

    else:
        logging.error(f"Failed to download feed. Status code: {response.status_code}")
        return None


