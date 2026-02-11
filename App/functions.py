import pandas as pd  # Importing pandas for data manipulation

# Function to load the dataset from a given file path
def load_dataset(file_path):
    """
    Load dataset from the given file path.

    Parameters:
    file_path (str): Path to the dataset file (CSV or Excel)

    Returns:
    DataFrame or str: Loaded dataset as a Pandas DataFrame or an error message
    """
    try:
        if file_path.endswith('.csv'):  # Check if the file is a CSV
            data = pd.read_csv(file_path, encoding='latin1')
        elif file_path.endswith('.xlsx'):  # Check if the file is an Excel file
            data = pd.read_excel(file_path, encoding='latin1')
        else:
            return "Unsupported file format. Please use .csv or .xlsx."
            
        print(f"Dataset loaded successfully with {data.shape[0]} rows and {data.shape[1]} columns.")
        return data  # Return the loaded dataset
    except FileNotFoundError:
        return f"Error: File not found at {file_path}."  # Handle file not found error
    except pd.errors.EmptyDataError:
        return "Error: The file is empty."  # Handle empty file error
    except Exception as e:
        return f"An unexpected error occurred: {e}"  # Handle unexpected errors

# Function to calculate the highest ROI (Return on Investment)
def highest_roi(data):
    try:
        # Calculate ROI: baseRent divided by totalRent, multiplied by 100 to get percentage
        data['ROI'] = data['baseRent'] / data['totalRent'] * 100

        # Group by 'city', calculate the mean ROI, and sort by ROI in descending order
        top_roi_areas = data.groupby('city')['ROI'].mean().sort_values(ascending=False).head(10)
        
        return top_roi_areas  # Return the top 10 areas with the highest ROI
    except KeyError as e:
        print(f"Missing required column: {e}")  # Handle missing column error
        return None
    except Exception as e:
        print(f"An error occurred while calculating ROI: {e}")  # Handle general errors
        return None

# Function to calculate the most common rental price range
def common_rental_price_range(data):
    try:
        # Define rental price bins
        bins = [0, 500, 1000, 1500, 2000, 3000, 5000]
        data['rent_range'] = pd.cut(data['totalRent'], bins=bins, labels=['0-500', '501-1000', '1001-1500', '1501-2000', '2001-3000', '3001-5000'])
        
        # Get the most common rental price range
        most_common_range = data['rent_range'].value_counts().idxmax()
        return most_common_range  # Return the most common rental price range
    except KeyError:
        print("Error: 'totalRent' column is missing in the dataset.")  # Handle missing 'totalRent' column
    except Exception as e:
        print(f"An error occurred while calculating rental price range: {e}")  # Handle general errors

# Function to calculate top ZIP codes with highest rent increases
def top_rent_increases(data):
    try:
        if 'yearConstructed' in data.columns:
            data['year'] = data['yearConstructed']  # Add 'year' column based on 'yearConstructed'
        # Calculate the percentage change in rent for each ZIP code and year
        rent_increase = data.groupby(['geo_plz', 'year'])['totalRent'].mean().pct_change().dropna()
        
        # Get the top 10 ZIP codes with the highest rent increases
        top_zip_codes = rent_increase.groupby('geo_plz').mean().sort_values(ascending=False).head(10)
        return top_zip_codes  # Return the top 10 ZIP codes with the highest rent increases
    except KeyError as e:
        print(f"Missing required column: {e}")  # Handle missing column error
    except Exception as e:
        print(f"An error occurred while calculating rent increases: {e}")  # Handle general errors

# Function to determine the most common heating type
def most_common_heating_type(data):
    try:
        # Get the most common heating type
        common_heating = data['heatingType'].value_counts().idxmax()
        return common_heating  # Return the most common heating type
    except KeyError:
        print("Error: 'heatingType' column is missing in the dataset.")  # Handle missing 'heatingType' column
    except Exception as e:
        print(f"An error occurred while determining the most common heating type: {e}")  # Handle general errors

# Function to calculate the average rent per square foot in top cities
def avg_rent_per_sqft_top_cities(data):
    try:
        # Calculate rent per square foot: baseRent divided by livingSpace
        data['rent_per_sqft'] = data['baseRent'] / data['livingSpace']
        
        # Get the top 5 cities based on the count of properties
        top_cities = data.groupby('city')['totalRent'].count().sort_values(ascending=False).head(5).index
        
        # Calculate average rent per square foot for these top cities
        avg_rent = data[data['city'].isin(top_cities)].groupby('city')['rent_per_sqft'].mean()
        return avg_rent  # Return the average rent per square foot for top cities
    except KeyError as e:
        print(f"Missing required column: {e}")  # Handle missing column error
    except Exception as e:
        print(f"An error occurred while calculating rent per square foot: {e}")  # Handle general errors