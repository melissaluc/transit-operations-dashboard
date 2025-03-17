def set_gdf_crs(gdf, crs=4326):
    """
    Set the CRS for a GeoDataFrame
    """
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=crs)
    return gdf

def create_route_shape_gdf(route_shapes_df):
    """
    Create a GeoDataFrame from a DataFrame of route shapes LineString geometry
    """
    route_shapes_df = route_shapes_df.drop_duplicates(subset=['shape_id', 'shape_pt_sequence'])
    route_shapes_filtered_gdf = gpd.GeoDataFrame(
        route_shapes_df.groupby('shape_id').agg(
            geometry=('shape_pt_sequence', lambda x: LineString(
                zip(
                    route_shapes_df.loc[x.index, 'shape_pt_lon'], 
                    route_shapes_df.loc[x.index, 'shape_pt_lat']
                )
            ))
        ).reset_index(), geometry='geometry')
    return route_shapes_filtered_gdf

def create_stop_points_gdf(stops_df):
    """
    Create a GeoDataFrame from given lon, lat
    """
    stops_gdf = gpd.GeoDataFrame(
        stops_df,
        geometry=gpd.points_from_xy(stops_df['stop_lon'], stops_df['stop_lat'])
    )
    return stops_gdf