import pandas as pd
import csv
from prophet import Prophet
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np
import calendar
import matplotlib.pyplot as plt
import os

class CrimePredictionModelProphet:
    def __init__(self):
        self.models = {}

    def load_data(self, dataframe, unemployment_file=None):
        self.df = dataframe.copy()
        self.unemployment_file = unemployment_file  # Assign before aggregate_data
        self.clean_data()
        self.aggregate_data()
        return self.df_agg

    def clean_data(self):
        date_columns = [col for col in self.df.columns if "date" in col.lower() or "date occ" in col.lower()]
        if not date_columns:
            raise ValueError("Không tìm thấy cột ngày tháng trong dữ liệu. Vui lòng kiểm tra file CSV.")
        date_col = date_columns[0]
        try:
            # Specify date format if known (e.g., '%m/%d/%Y %H:%M:%S %p' for Chicago crime data)
            self.df['DATE_OCC'] = pd.to_datetime(self.df[date_col], format='%m/%d/%Y %H:%M:%S %p', errors='coerce')
            self.df['month'] = self.df['DATE_OCC'].dt.month
            self.df['year'] = self.df['DATE_OCC'].dt.year
            self.df['day_of_week'] = self.df['DATE_OCC'].dt.dayofweek
        except Exception as e:
            print(f"Lỗi khi xử lý ngày tháng: {e}")
            self.df['DATE_OCC'] = pd.NaT
            self.df['month'] = pd.NA
            self.df['year'] = pd.NA
            self.df['day_of_week'] = pd.NA

        area_columns = [col for col in self.df.columns if "area" in col.lower() or "community area" in col.lower()]
        if not area_columns:
            raise ValueError("Không tìm thấy cột khu vực trong dữ liệu. Vui lòng kiểm tra file CSV.")
        area_col = area_columns[0]
        try:
            self.df['AREA'] = self.df[area_col].astype(str).str.replace(r'\.0$', '', regex=True)
            self.df = self.df[self.df['AREA'] != 'nan']
        except Exception as e:
            print(f"Lỗi khi xử lý khu vực: {e}")
            self.df['AREA'] = pd.NA

        required_columns = ['AREA', 'month']
        if not all(col in self.df.columns for col in required_columns):
            missing_cols = [col for col in required_columns if col not in self.df.columns]
            raise ValueError(f"Thiếu cột cần thiết: {missing_cols}")
        
        self.df = self.df.dropna(subset=required_columns)
        return self.df

    def aggregate_data(self):
        self.df_agg = self.df.groupby(['AREA', 'year', 'month']).size().reset_index(name='crime_count')
        self.df_agg['ds'] = pd.to_datetime(self.df_agg[['year', 'month']].assign(DAY=1), errors='coerce')
        
        self.df_agg['days_in_month'] = self.df_agg.apply(
            lambda x: calendar.monthrange(int(x['year']), int(x['month']))[1], axis=1
        )
        self.df_agg['month_sin'] = np.sin(2 * np.pi * self.df_agg['month'] / 12)
        self.df_agg['month_cos'] = np.cos(2 * np.pi * self.df_agg['month'] / 12)
        self.df_agg['day_of_week'] = self.df_agg['ds'].dt.dayofweek
        self.df_agg['day_of_week_sin'] = np.sin(2 * np.pi * self.df_agg['day_of_week'] / 7)
        self.df_agg['day_of_week_cos'] = np.cos(2 * np.pi * self.df_agg['day_of_week'] / 7)
        self.df_agg['is_holiday'] = self.df_agg['month'].isin([1, 7, 12]).astype(int)

        if self.unemployment_file and os.path.exists(self.unemployment_file):
            try:
                unemployment_data = pd.read_csv(self.unemployment_file)
                unemployment_data['year'] = unemployment_data['year'].astype(int)
                unemployment_data['month'] = unemployment_data['month'].astype(int)
                unemployment_data['AREA'] = unemployment_data['AREA'].astype(str)
                self.df_agg = self.df_agg.merge(
                    unemployment_data[['year', 'month', 'AREA', 'unemployment_rate']],
                    on=['year', 'month', 'AREA'],
                    how='left'
                )
                self.df_agg['unemployment_rate'] = self.df_agg['unemployment_rate'].fillna(5.0)
            except Exception as e:
                print(f"Lỗi khi ánh xạ dữ liệu thất nghiệp: {e}")
                self.df_agg['unemployment_rate'] = 5.0
        else:
            print(f"Không tìm thấy file thất nghiệp: {self.unemployment_file}. Sử dụng giá trị mặc định 5.0.")
            self.df_agg['unemployment_rate'] = 5.0

        return self.df_agg

    def train_model(self, test_size=0.2):
        for area in self.df_agg['AREA'].unique():
            area_data = self.df_agg[self.df_agg['AREA'] == area][
                ['ds', 'crime_count', 'month', 'year', 'days_in_month', 'month_sin', 'month_cos', 
                 'day_of_week_sin', 'day_of_week_cos', 'is_holiday', 'unemployment_rate']
            ].rename(columns={'crime_count': 'y'})
            
            print(f"Khu vực {area}: Số điểm dữ liệu = {len(area_data)}")
            
            if len(area_data) > 2:
                train_data, test_data = train_test_split(area_data, test_size=test_size, shuffle=False)
                model = Prophet(yearly_seasonality=True, weekly_seasonality=True, seasonality_mode='multiplicative')
                
                for regressor in ['month', 'year', 'days_in_month', 'month_sin', 'month_cos', 
                                  'day_of_week_sin', 'day_of_week_cos', 'is_holiday', 'unemployment_rate']:
                    model.add_regressor(regressor)
                
                model.fit(train_data)
                self.models[area] = model

                future = test_data[['ds', 'month', 'year', 'days_in_month', 'month_sin', 'month_cos', 
                                    'day_of_week_sin', 'day_of_week_cos', 'is_holiday', 'unemployment_rate']]
                forecast = model.predict(future)
                mse = mean_squared_error(test_data['y'], forecast['yhat'])
                print(f"Khu vực {area}: MSE trên tập test = {mse:.2f}")
            else:
                print(f"Không đủ dữ liệu để huấn luyện Prophet cho khu vực {area}")

    def predict(self, area, year, month, unemployment_rate=None):
        if area not in self.models:
            raise ValueError(f"Không tìm thấy mô hình cho khu vực {area}. Vui lòng huấn luyện mô hình trước.")

        days_in_month = calendar.monthrange(year, month)[1]
        first_day_of_month = pd.to_datetime(f'{year}-{month}-01')
        
        future = pd.DataFrame({
            'ds': [first_day_of_month],
            'month': [month],
            'year': [year],
            'days_in_month': [days_in_month],
            'month_sin': [np.sin(2 * np.pi * month / 12)],
            'month_cos': [np.cos(2 * np.pi * month / 12)],
            'day_of_week_sin': [np.sin(2 * np.pi * first_day_of_month.dayofweek / 7)],
            'day_of_week_cos': [np.cos(2 * np.pi * first_day_of_month.dayofweek / 7)],
            'is_holiday': [1 if month in [1, 7, 12] else 0],
            'unemployment_rate': [unemployment_rate]
        })
        forecast = self.models[area].predict(future)
        return max(forecast['yhat'][0], 0)  # Ensure non-negative prediction

    def visualize_forecast(self, areas=None, start_year=2025, start_month=5, num_months=10, unemployment_data=None):
        if areas is None:
            areas = list(self.models.keys())
        else:
            areas = [area for area in areas if area in self.models]

        if not areas:
            print("Không có khu vực nào để visualize. Vui lòng huấn luyện mô hình trước")
            return

        dates = []
        current_year, current_month = start_year, start_month
        for _ in range(num_months):
            dates.append(pd.to_datetime(f"{current_year}-{current_month:02d}-01"))
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

        forecast_data = {}
        for area in areas:
            forecast_data[area] = [
                self.predict(area, date.year, date.month, unemployment_data) for date in dates
            ]

        plt.figure(figsize=(10, 6))
        colors = ['#FF5733', '#33FF57', '#3357FF', '#FF33A1', '#33FFF5', '#F5FF33']
        for i, area in enumerate(areas):
            plt.plot(dates, forecast_data[area], label=f'Khu vực {area}', color=colors[i % len(colors)], 
                     linewidth=2, marker='o', markersize=6)

        plt.title('Dự đoán số vụ phạm tội trong các tháng tiếp theo ở Chicago', fontsize=16, pad=15)
        plt.xlabel('Thời gian (Năm-Tháng)', fontsize=12)
        plt.ylabel('Số vụ phạm tội dự đoán', fontsize=12)
        plt.xticks(dates, [date.strftime('%Y-%m') for date in dates], rotation=45)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend()
        plt.tight_layout()
        plt.savefig('forecast_plot.png')
        plt.show()
if __name__ == "__main__":
    try:
        df = pd.read_csv(
            "US-chicago_2020To-Present_part-00000-a4109e43-f86f-4d03-9f12-4805055ffcca-c000.csv",
            on_bad_lines='skip',  # Skip malformed lines
            quoting=csv.QUOTE_ALL,
            low_memory=False
        )
    except FileNotFoundError:
        print("Lỗi: Không tìm thấy file dữ liệu tội phạm. Vui lòng kiểm tra đường dẫn.")
        exit(1)

    # Create unemployment data if file doesn't exist
    unemployment_file = "unemployment_data.csv"

    model_prophet = CrimePredictionModelProphet()
    model_prophet.load_data(df, unemployment_file)
    model_prophet.train_model(test_size=0.2)

    area_predict = "1"
    year_predict = 2025
    month_predict = 12
    prediction = model_prophet.predict(area_predict, year_predict, month_predict, 0.047)
    print(f"\nDự đoán số vụ phạm tội cho khu vực {area_predict} vào tháng {month_predict}/{year_predict}: {prediction:.2f}")
