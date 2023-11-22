from io import StringIO
import pandas as pd
import requests
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay

# Function to retrieve daily historical data
def get_daily_data(symbol, interval, start_date, end_date, api_token):

    base_url = f"https://eodhd.com/api/eod/{symbol}?api_token={api_token}&period={interval}&from={start_date}&to={end_date}&fmt=csv"

    print(f"baseurl: {base_url}")
    response = requests.get(base_url)
    if response.status_code == 200:
        # Read CSV data from the API response
        data = pd.read_csv(StringIO(response.text))  # Use io.StringIO
        return data
    else:
        print(f"Failed to fetch data for symbol: {symbol}")
        print(response.text)
        return None

# Function to retrieve 10 years of 15-minute data for a list of symbols
def get_intraday_data(symbol, timeframe, key):

    url = f'https://eodhd.com/api/intraday/{symbol}?interval={timeframe}&api_token={key}&fmt=csv'
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Convert the CSV content to a DataFrame
        data = pd.read_csv(StringIO(response.text))

        # Drop the 'Gmtoffset' column
        data = data.drop(['Gmtoffset', 'Datetime'], axis=1)

        # Convert the 'Timestamp' column to datetime in the Eastern timezone
        data['Timestamp'] = pd.to_datetime(data['Timestamp'], unit='s')
        data['Timestamp'] = data['Timestamp'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
        
        # Drop rows containing NaN values
        data = data.dropna()

    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")

    # Display the last 40 rows of the DataFrame
    return data

# To adjust the Open, High, Low, and Close prices in the stock_data DataFrame based on the stock split 
def split_adjust_daily(stock_data, symbol, key, start_date):

    url = f'https://eodhd.com/api/splits/{symbol}?from={start_date}&api_token={key}&fmt=json'

    data = requests.get(url).json()

    # Create a DataFrame from the JSON data
    data = pd.DataFrame(data)

    # Rename columns to start with an uppercase letter
    data.columns = data.columns.map(lambda x: x.capitalize())

    if not data.empty:

        # Assuming 'data' is your DataFrame
        data['Date'] = pd.to_datetime(data['Date'])

        # Calculate the business day offset for shifting
        business_day_offset = BDay(1)

        # Shift the 'Date' column to get the previous business day's date
        data['Previous_Date'] = data['Date'] - business_day_offset

        # Replace the 'Date' column with the previous day's date
        data['Date'] = data['Previous_Date'].fillna(data['Date'])

        # Drop the 'Previous_Date' column if not needed
        data = data.drop('Previous_Date', axis=1)

        # Convert 'Date' column to datetime format
        #data['Date'] = pd.to_datetime(data['Date'])
        stock_data['Date'] = pd.to_datetime(stock_data['Date'])

        # Extract split ratios
        data[['Split_Numerator', 'Split_Denominator']] = data['Split'].str.split('/', expand=True).astype(float)

        # Merge data with df_split based on the 'Date' column
        merged_data = pd.merge(stock_data, data[['Date', 'Split_Numerator', 'Split_Denominator']], on='Date', how='left')

        # Back fill split ratios to fill NaN values
        merged_data[['Split_Numerator', 'Split_Denominator']] = merged_data[['Split_Numerator', 'Split_Denominator']].bfill()

        # Forward fill the Split_Numerator and Split_Denominator with 1s 
        merged_data[['Split_Numerator', 'Split_Denominator']] = merged_data[['Split_Numerator', 'Split_Denominator']].fillna(1)

        # Adjust Open, High, Low, and Close prices based on stock splits only for dates on or before the split date
        adjusted_columns = ['Open', 'High', 'Low', 'Close']

        for col in adjusted_columns:
            merged_data[col] = (merged_data[col] * (merged_data['Split_Denominator'] / merged_data['Split_Numerator'])).round(2)

        # Drop unnecessary columns
        merged_data.drop(['Split_Numerator', 'Split_Denominator'], axis=1, inplace=True)

        # Return the updated dataframe
        return merged_data
    else:
        # Return the original DataFrame if there were no stock splits for the symbol
        return stock_data


# To adjust the Open, High, Low, and Close prices in the stock_data DataFrame based on the stock split 
def split_adjust_intraday(stock_data, symbol, key, start_date):

    url = f'https://eodhd.com/api/splits/{symbol}?from={start_date}&api_token={key}&fmt=json'

    data = requests.get(url).json()

    # Create a DataFrame from the JSON data
    data = pd.DataFrame(data)

    # Rename columns to start with an uppercase letter
    data.columns = data.columns.map(lambda x: x.capitalize())

    if not data.empty:
        # Calculate the business day offset for shifting
        business_day_offset = BDay(1)

        # Convert 'Date' column to datetime format
        data['Date'] = pd.to_datetime(data['Date']).dt.tz_localize(None)  # Remove timezone information

        # Shift the 'Date' column to get the previous business day's date
        data['Previous_Date'] = data['Date'] - business_day_offset

        # Replace the 'Date' column with the previous day's date
        data['Date'] = data['Previous_Date'].fillna(data['Date'])

        # Drop the 'Previous_Date' column if not needed
        data = data.drop('Previous_Date', axis=1)

        # Convert 'Date' column to datetime format
        stock_data['Timestamp'] = pd.to_datetime(stock_data['Timestamp']).dt.tz_localize(None)

        # Extract split ratios
        data[['Split_Numerator', 'Split_Denominator']] = data['Split'].str.split('/', expand=True).astype(float)

        # Merge data with df_split based on the 'Date' column
        merged_data = pd.merge(stock_data, data[['Date', 'Split_Numerator', 'Split_Denominator']], left_on='Timestamp', right_on='Date', how='left')

        # Back fill split ratios to fill NaN values
        merged_data[['Split_Numerator', 'Split_Denominator']] = merged_data[['Split_Numerator', 'Split_Denominator']].bfill()

        # Forward fill the Split_Numerator and Split_Denominator with 1s 
        merged_data[['Split_Numerator', 'Split_Denominator']] = merged_data[['Split_Numerator', 'Split_Denominator']].fillna(1)

        # Adjust Open, High, Low, and Close prices based on stock splits only for dates on or before the split date
        adjusted_columns = ['Open', 'High', 'Low', 'Close']

        for col in adjusted_columns:
            merged_data[col] = (merged_data[col] * (merged_data['Split_Denominator'] / merged_data['Split_Numerator'])).round(2)

        # Drop unnecessary columns
        merged_data.drop(['Split_Numerator', 'Split_Denominator', 'Date'], axis=1, inplace=True)

        # Return the updated dataframe
        return merged_data
    else:
        # Return the original DataFrame if there were no stock splits for the symbol
        return stock_data

def main():

    # Read the API key
    key = open('api_token.txt').read()

    # Number of days back is used when requesting historical data
    # This parameter is used when requesting historical daily data
    daysBack = 365 * 2

    # Intraday timeframe
    timeframe='1h'

    # Get today's date in the 'YYYY-MM-DD' format
    # and the start date (daysBack)
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=daysBack)

    # Open the symbols.csv and read the stock symbols
    with open('symbols.csv') as f:
        lines = f.read().splitlines()
        for symbol in lines:
        
            # Get stock historical daily data - going back daysBack days
            ##data = get_daily_data(symbol, 'd', start_date, end_date, key)
            # Adjust the Open, High, Low, and Close prices in the data DataFrame based on the stock split 
            ##data = split_adjust_daily(data, symbol, key, start_date)
            # Write the dataframe with the stock data to a CSV file
            ##data.to_csv("datasets/{}.csv".format(symbol))

            # Get historical intraday data
            data = get_intraday_data(symbol, timeframe, key)
            # Adjust the Open, High, Low, and Close prices in the data DataFrame based on the stock split 
            data = split_adjust_intraday(data, symbol, key, start_date)
            # Write the dataframe with the stock data to a CSV file
            data.to_csv(f"dataset_intraday/{symbol}_{timeframe}.csv")


if __name__ == '__main__':
    main()

    