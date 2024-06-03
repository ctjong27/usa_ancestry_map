import os
import pandas as pd
import geopandas as gpd
import numpy as np
import random
import logging

def process_census_data(multiple_ancestries_columns, base_directory, csv_file_name, shapefile_directory, output_file_name, cutoff_keyword):
    try:
        # Set random seed for reproducibility
        random.seed(42)

        # Set display options to show all columns
        pd.set_option('display.max_columns', None)

        # Read and clean US Census data
        csv_file_path = os.path.join(base_directory, '_input_raw_data', 'social_explorer_tract_ancestry_data', csv_file_name)
        ancestry_df = pd.read_csv(csv_file_path)

        # Drop the 0th row and reindex
        ancestry_df = ancestry_df.drop(ancestry_df.index[0]).reset_index(drop=True)

        # Filter for columns before and including "Census Tract", and columns including and after "Total Population"
        start_col = "Census Tract"
        end_col = "Total Population"

        # Get column indices
        start_idx = ancestry_df.columns.get_loc(start_col)
        end_idx = ancestry_df.columns.get_loc(end_col)

        # Select the columns
        selected_columns = ancestry_df.columns[:start_idx + 1].tolist() + ancestry_df.columns[end_idx:].tolist()
        filtered_df = ancestry_df[selected_columns]

        # Convert relevant columns to numeric
        numeric_columns = filtered_df.columns[filtered_df.columns.get_loc("Total Population:") + 1:]
        filtered_df[numeric_columns] = filtered_df[numeric_columns].apply(pd.to_numeric, errors='coerce')

        # Read shapefile data
        shapefile_path = os.path.join(base_directory, '_input_raw_data', 'us_census_cartography', shapefile_directory)
        shapefile = gpd.read_file(shapefile_path)

        # Merge geospatial data and US Census data
        filtered_df['FIPS'] = pd.to_numeric(filtered_df['FIPS'], errors='coerce').astype(np.int64)
        shapefile['GEOID'] = pd.to_numeric(shapefile['GEOID'], errors='coerce').astype(np.int64)
        merged_df = pd.merge(filtered_df, shapefile, left_on='FIPS', right_on='GEOID', how='inner')

        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(merged_df, geometry='geometry')

        # List to store the dot data
        dots_data = []

        # Function to generate random points within a polygon
        def generate_random_points(polygon, num_points):
            points = []
            minx, miny, maxx, maxy = polygon.bounds
            while len(points) < num_points:
                random_point = gpd.points_from_xy([random.uniform(minx, maxx)], [random.uniform(miny, maxy)])[0]
                if polygon.contains(random_point):
                    points.append(random_point)
            return points

        # Generate dots for each column and store in dots_data
        for column in multiple_ancestries_columns:
            cutoff_col = column.split(cutoff_keyword)[-1].strip()
            for _, row in gdf.iterrows():
                try:
                    num_dots = int(float(row[column]) / 15)  # 1 dot per 15 persons
                    if num_dots > 0 and not row['geometry'].is_empty:
                        random_points = generate_random_points(row['geometry'], num_dots)
                        for point in random_points:
                            dots_data.append({'column': cutoff_col, 'latitude': point.y, 'longitude': point.x})
                except ValueError:
                    continue

        # Create a DataFrame from dots_data
        dots_df = pd.DataFrame(dots_data)

        # Export to CSV for Tableau
        output_file_path = os.path.join(base_directory, output_file_name)
        dots_df.to_csv(output_file_path, index=False)

        print(f"Data exported to {output_file_path}")
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")