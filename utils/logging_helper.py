import logging


def log_dataframe_dict_keys_dimension(data_dict):
    """
    For the purpose of logging returned data
    """
    logging.info(f"Returning data dict with keys: {list(data_dict.keys())}")
    for key, value in data_dict.items():
        logging.info(f"DataFrame {key} containing {len(value)} rows of data")