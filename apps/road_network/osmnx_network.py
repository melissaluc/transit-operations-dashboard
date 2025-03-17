import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, MultiPoint
import osmnx as ox
import numpy as np

import logging


def get_road_network_attributes(PLACE_NAME="Toronto, Ontario, Canada", boundary_df=False):
    """
    Return road network edge attributes
    """

    logging.info(f"Generating network edges for {PLACE_NAME}")
    # Get the street network
    street_network = ox.graph_from_place(PLACE_NAME, network_type='drive')

    # Get the polygon boundary for Toronto
    gdf_city_boundary = ox.geocode_to_gdf(PLACE_NAME)

    # Create the road attribute DataFrame
    data = []
    for u, v, key, attr in street_network.edges(keys=True, data=True):
        maxspeed = attr.get('maxspeed', np.nan)
        if isinstance(maxspeed, list):
            maxspeed = ', '.join(maxspeed)
        data.append({
            'u': u,
            'v': v,
            'key': key,
            'road_name': attr.get('name', 'unknown'),
            'highway': attr.get('highway', 'unknown'),
            'maxspeed': maxspeed
        })

    df_road_network = pd.DataFrame(data)

    # Convert the DataFrame to a GeoDataFrame
    edges_gdf = ox.graph_to_gdfs(street_network, nodes=False, edges=True)
    edges_gdf = edges_gdf.merge(df_road_network, on=['u', 'v', 'key'])

    logging.info(f"Returning road network edges {'and boundary polygon' if boundary_df is True else ''}")
    return edges_gdf, gdf_city_boundary if boundary_df is True else edges_gdf

