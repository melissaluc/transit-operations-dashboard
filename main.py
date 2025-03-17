import apps.gtfs.gtfs_static as gtfs_static
import apps.road_network as get_road_network_attributes
from apps.traffic_signals.traffic_signal_timing import fetch_traffic_signal_timing_data
from apps.traffic_signals.traffic_signal_locations import fetch_traffic_signal_locations_data

from utils.geoprocessing import set_gdf_crs, create_route_shape_gdf, create_stop_points_gdf

import os
import django
import logging
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
import matplotlib.pyplot as plt
from datetime import datetime


# Set the settings module for Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'transit_dashboard_data.settings')
django.setup()


logging.basicConfig(
    filename='app.log', 
    level=logging.DEBUG,  
    format='%(asctime)s - %(levelname)s - %(message)s' 
)

def plot_trips(merged_gdf, route_name):
    fig, ax = plt.subplots()

    for trip_id, trip_data in merged_gdf.groupby('trip_id'):
        # Plot arrival times
        ax.plot(trip_data['arrival_time'], trip_data['shape_dist_traveled'], label=f"{trip_data['trip_headsign']} (arrival)", linestyle='--')
        # Plot departure times
        ax.plot(trip_data['departure_time'], trip_data['shape_dist_traveled'], linestyle='--')


    ax.set_xlabel('Time')
    ax.set_ylabel('Shape Distance Traveled')
    ax.set_title(f"{route_name}")
    ax.legend(title=f"{route_name}", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45)
    plt.show()

def filter_by_route_id(trips_df, ROUTE_ID, TRIP_HEADSIGN: list | None =None):
    """
    Filter trips df by route_id and direction returning the selected routes and other routes
    \nselected routes: the route selected in one direction
    \nother routes: all routes but the selected route, direction and overlaps not considered
    """
    if TRIP_HEADSIGN is not None:
        selected_route_df = trips_df[(trips_df['route_id'] == ROUTE_ID) & (trips_df['trip_headsign'].isin(TRIP_HEADSIGN))]
    else:
        selected_route_df = trips_df[trips_df['route_id'] == ROUTE_ID]

    other_routes_df = trips_df[trips_df['route_id'] != ROUTE_ID]
    return selected_route_df, other_routes_df

def filter_route_shapes(route_shapes_df, selected_route_df, other_routes_df):
    selected_route_shapes_df = route_shapes_df[route_shapes_df['shape_id'].isin(selected_route_df['shape_id'])]
    other_route_shapes_df = route_shapes_df[route_shapes_df['shape_id'].isin(other_routes_df['shape_id'])]
    return selected_route_shapes_df, other_route_shapes_df

def get_route_shape_gdfs(selected_route_shapes_df, other_route_shapes_df):
    selected_route_shapes_gdf = create_route_shape_gdf(selected_route_shapes_df)
    trips_filtered_gdf = create_route_shape_gdf(other_route_shapes_df)
    return selected_route_shapes_gdf, trips_filtered_gdf

def buffer_route_shape(selected_route_shapes_gdf):
    selected_route_shapes_gdf = set_gdf_crs(selected_route_shapes_gdf)
    logging.info(f"Selected Route Shapes GeoDataFrame: {selected_route_shapes_gdf}")
    
    # Re-project to a projected CRS
    # TODO: function that determines crs for transit agency area
    projected_gdf = selected_route_shapes_gdf.to_crs(epsg=32617) 
    selected_route_buffered = projected_gdf.buffer(100)  # Buffer distance in meters
    selected_route_buffered_gdf = gpd.GeoDataFrame(geometry=selected_route_buffered, crs=projected_gdf.crs)
    
    # Re-project back to the original CRS
    selected_route_buffered_gdf = selected_route_buffered_gdf.to_crs(epsg=4326)
    logging.info(f"Buffered GeoDataFrame: {selected_route_buffered_gdf}")
    return selected_route_buffered_gdf

def calculate_angle(line):
    coords = list(line.coords)
    start_point = coords[0]
    end_point = coords[-1]
    dx = end_point[0] - start_point[0]
    dy = end_point[1] - start_point[1]
    angle = math.degrees(math.atan2(dy, dx))
    return angle

def filter_opposing_directions(gdf, reference_angle, tolerance=10):
    gdf['angle'] = gdf['geometry'].apply(calculate_angle)
    def is_opposing(angle):
        return not (reference_angle - tolerance <= angle <= reference_angle + tolerance)

    filtered_gdf = gdf[gdf['angle'].apply(is_opposing)]
    return filtered_gdf

def perform_overlay_analysis(trips_filtered_gdf, selected_route_buffered_gdf, other_routes_df):
    logging.info(f"Trips Filtered GeoDataFrame: {trips_filtered_gdf}")
    logging.info(f"Selected Route Buffered GeoDataFrame: {selected_route_buffered_gdf}")
    route_overlap_gdf = gpd.overlay(trips_filtered_gdf, selected_route_buffered_gdf, how='intersection')
    if not route_overlap_gdf.empty:
        threshold = 0.05
        true_overlapping_routes = route_overlap_gdf[route_overlap_gdf.geometry.apply(lambda x: x.length > threshold)]
        true_overlapping_routes = true_overlapping_routes.drop_duplicates(subset=['shape_id', 'geometry'])
        other_trips = other_routes_df[other_routes_df['shape_id'].isin(true_overlapping_routes['shape_id'])]
        return true_overlapping_routes, other_trips
    else:
        logging.info('No overlapping trips found')
        return gpd.GeoDataFrame(), pd.DataFrame()

def filter_stop_times(stop_times_df, selected_route_df, other_trips):
    selected_route_stop_times_df = stop_times_df[stop_times_df['trip_id'].isin(selected_route_df['trip_id'])]
    other_stop_times_df = stop_times_df[stop_times_df['trip_id'].isin(other_trips['trip_id'])]
    return selected_route_stop_times_df, other_stop_times_df

def merge_stops_with_stop_times(stop_times_df, stops_gdf):
    merged_gdf = pd.merge(stop_times_df, stops_gdf, on='stop_id')
    merged_gdf = merged_gdf.assign(
            arrival_time=pd.to_datetime(merged_gdf['arrival_time'], format='%H:%M:%S'),
            departure_time=pd.to_datetime(merged_gdf['departure_time'], format='%H:%M:%S')
        )
    merged_gdf['dwell'] = merged_gdf['departure_time'] - merged_gdf['arrival_time']
    merged_gdf['shape_dist_traveled'] = merged_gdf['shape_dist_traveled'].fillna(value=0)
    merged_gdf = merged_gdf.sort_values(by=['trip_id', 'stop_sequence'])
    return merged_gdf

def merge_stop_times_with_trip(stop_times_df, trips_df):
    merged_df = pd.merge(stop_times_df,trips_df, on='trip_id')
    return merged_df


def process_agency_feed(agency, feed, ROUTE_ID=None, TRIP_HEADSIGN: list | None = None):
    logging.info(f"Processing {agency} {feed['date']} GTFS feed, filtering for route_id: {ROUTE_ID}")

    # Load feed data into dataframes
    route_shapes_df = feed['data']['shapes']
    routes_df = feed['data']['routes']
    trips_df = feed['data']['trips']
    stops_df = feed['data']['stops']
    stop_times_df = feed['data']['stop_times']

    # Get route information
    route_row = routes_df[routes_df['route_id'] == ROUTE_ID].iloc[0]
    ROUTE_NAME = f"{route_row['route_short_name']} {route_row['route_long_name']} {TRIP_HEADSIGN[0] if TRIP_HEADSIGN is not None else ''}"

    # Split route and trips between selected route (by direction and route id) and potential overlapping routes
    selected_route_df, other_routes_df = filter_by_route_id(trips_df, ROUTE_ID)
    selected_route_shapes_df, other_route_shapes_df = filter_route_shapes(route_shapes_df, selected_route_df, other_routes_df)

    # Generate route shapes for overlap analysis
    selected_route_shapes_gdf, trips_filtered_gdf = get_route_shape_gdfs(selected_route_shapes_df, other_route_shapes_df)
    selected_route_shapes_gdf =set_crs(selected_route_shapes_gdf, crs=4326)
    trips_filtered_gdf = set_crs(trips_filtered_gdf, crs=4326)
    selected_route_buffered_gdf = buffer_route_shape(selected_route_shapes_gdf)
    
    # Overlay analysis
    true_overlapping_routes, other_trips = perform_overlay_analysis(trips_filtered_gdf, selected_route_buffered_gdf, other_routes_df)
    
    selected_route_stop_times_df, other_stop_times_df = filter_stop_times(stop_times_df, selected_route_df, other_trips)

    selected_route_stops_gdf = create_stop_points_gdf(stops_df[stops_df['stop_id'].isin(selected_route_stop_times_df['stop_id'])])
    merged_stop_times_with_trips_gdf = merge_stop_times_with_trip( selected_route_stop_times_df, selected_route_df)
    merged_gdf = merge_stops_with_stop_times(merged_stop_times_with_trips_gdf, selected_route_stops_gdf)
    
    
    # plot_trips(merged_gdf, ROUTE_NAME)


def filter_key_points():
    """
    Remove 
    """



def get_segment_bearing(p1, p2):
    """
    Calculate bearing of a linestring
    """
    angle = np.arctan2(p2.y - p1.y, p2.x - p1.x)
    bearing = np.degrees(angle)
    if bearing < 0:
        bearing += 360
    return bearing

def main():
    """
    Return necessary data to plot space-time diagram
    \nStatic GTFS: distance\chainage, key location markers (stops/stations/intersections (stop bar)), timestamps
    \nReal-time GTFS: distance\chainage, key location markers (stops/stations/intersections (stop bar)), timestamps
    \nRoad network attributes: speed
    \nTraffic Signal Timing Plans assume FSHW convention (ph 2 (SB) and 6 (NB) for thru movement main street)
    """
    
    # # User input: AREA
    gtfs_static_dataset = gtfs_static.main(
        AGENCY='Toronto Transit Commission',
        COUNTRY_CODE='CA',
        SUBDIVISION_NAME=None,
        MUNICIPALITY=None,
    )
    # Determine line segments first as it is the reference for all key points to keep order of the route
    # Line segments are defined from stop i to stop i+1
    # Get Municipal Traffic Signal Timing filtered for the route with buffered route

    traffic_signal_timing_data = fetch_traffic_signal_timing_data()
    traffic_signal_locations_data = fetch_traffic_signal_locations_data()
    road_network = get_road_network_attributes(PLACE_NAME="Toronto, Ontario, Canada", boundary_df=False)



    # TRIP_HEADSIGN = ['EAST - 10 VAN HORNE towards VICTORIA PARK', 'NOT IN SERVICE']
    # ROUTE_ID = 72934 
    # AGENCY='Toronto Transit Commission'
    # for agency, feed in gtfs_static_dataset.items():
    #     # User input: ROUTE_ID and TRIP_HEADSIGN
    #     process_agency_feed(agency, feed, ROUTE_ID, TRIP_HEADSIGN)

if __name__ == "__main__":
    main()