import pandas as pd
import geopandas as gpd
import numpy as np
from scipy.stats import poisson
from shapely.geometry import Point, box

GRID_LOCATION = 'temp/grid.geojson'
BOUNDARY_LOCATION = "temp/chicago.geojson"
DATA_LOCATION = "temp/chicago.csv"
PREDICTION_LOCATION = 'temp/predict.geojson'


geo_borders = gpd.read_file(BOUNDARY_LOCATION)

def make_grid():
    try:
        grid = gpd.read_file(GRID_LOCATION)
    except:
        
        def create_grid(bounds, cell_size=0.006):
            xmin, ymin, xmax, ymax = bounds
            grid_cells = []
            grid_ids = []
            cell_id = 0
            for x in np.arange(xmin, xmax, cell_size):
                for y in np.arange(ymin, ymax, cell_size):
                    cell = box(x, y, x + cell_size, y + cell_size)
                    if geo_borders.geometry.intersects(cell).any():
                        grid_cells.append(cell)
                        grid_ids.append(cell_id)
                        cell_id += 1
            return gpd.GeoDataFrame({'grid_id': grid_ids, 'geometry': grid_cells}, crs=geo_borders.crs)
        bounds = geo_borders.total_bounds
        grid = create_grid(bounds)
        grid.to_file(GRID_LOCATION)
    return grid

def data_process_init(crime_data):
    crime_data['Date'] = pd.to_datetime(crime_data['Date'])
    crime_data = crime_data[crime_data['Longitude'] != 0]
    crime_data = crime_data[crime_data['Latitude'] != 0]

    grid = make_grid()

    crime_gdf = gpd.GeoDataFrame(
        crime_data,
        geometry=[Point(xy) for xy in zip(crime_data.Longitude, crime_data.Latitude)],
        crs=geo_borders.crs
    )
    crime_with_grid = gpd.sjoin(crime_gdf, grid, how='left', predicate='within')
    crime_with_grid = crime_with_grid.drop(columns=['index_right', 'Latitude', 'Longitude'])
    crime_with_grid['Date'] = pd.to_datetime(crime_with_grid['Date'])
    crime_with_grid['month'] = crime_with_grid['Date'].dt.to_period('M')
    crime_counts = crime_with_grid.groupby(['grid_id', 'month']).size().unstack(fill_value=0)
    
    return crime_counts

def calculate_true_positives(crime_counts, threshold=0.5):
    true_positives = pd.DataFrame(index=crime_counts.index, columns=crime_counts.columns)
    
    for grid_id in crime_counts.index:
        for i, month in enumerate(crime_counts.columns[12:], start=12):
            # Get past 12 months of crime counts
            past_12 = crime_counts.loc[grid_id, crime_counts.columns[i-12:i]]
            mean_crime = past_12.mean()
            
            # Calculate Poisson probability of zero crimes
            prob_zero = poisson.cdf(0, mean_crime)
            prob_crime = 1 - prob_zero
            
            # Check if crime occurred in the forecasted month
            actual_crime = crime_counts.loc[grid_id, month]
            
            # Assign true-positive (1) or false (0)
            if prob_crime > threshold and actual_crime > 0:
                true_positives.loc[grid_id, month] = 1
            else:
                true_positives.loc[grid_id, month] = 0
    
    # Calculate average true-positive rate per grid cell
    true_positives = true_positives.astype(float)
    avg_true_positives = true_positives.mean(axis=1)
    return avg_true_positives

def forecast_hot_spots(crime_counts, forecast_month, num_hot_spots):
    # Get crime counts for the previous month
    previous_month = pd.Period(forecast_month.start_time - pd.offsets.MonthBegin(), freq='M')
    
    if str(previous_month) not in crime_counts.columns:
        raise ValueError(f"Previous month {previous_month} not in data")
    
    avg_true_positives = calculate_true_positives(crime_counts)
    previous_crimes = crime_counts[str(previous_month)]
    
    # Combine true-positive rates and current crimes
    forecast_df = pd.DataFrame({
        'avg_true_positive': avg_true_positives,
        'previous_crimes': previous_crimes
    })
    
    forecast_df = forecast_df.sort_values(
        by=['avg_true_positive', 'previous_crimes'], 
        ascending=[False, False]
    )
    
    # Select top N grid cells as hot spots
    hot_spots = forecast_df.head(num_hot_spots).index
    return hot_spots


crime_data = pd.read_csv(DATA_LOCATION, on_bad_lines='skip')
forecast_month = pd.Period(pd.to_datetime(crime_data['Date'].max()) + pd.offsets.MonthBegin(), freq='M')
print("Prediction for ", str(forecast_month.month) + "/" + str(forecast_month.year))

num_hot_spots = 120

crime_counts = data_process_init(crime_data)
hot_spots = forecast_hot_spots(crime_counts, forecast_month, num_hot_spots)

grid = make_grid()

hotspots_gdf = grid[grid['grid_id'].isin(hot_spots)]

hotspots_gdf.to_file(PREDICTION_LOCATION)
print("Output written to: ", PREDICTION_LOCATION)