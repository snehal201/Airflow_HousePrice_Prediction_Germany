import pandas as pd


def load_dataset(file_path):
    """
    Load dataset from the given file path.

    Parameters:
    file_path (str): Path to the dataset file (CSV or Excel)

    Returns:
    DataFrame: Loaded dataset as a Pandas DataFrame
    """
    try:
        if file_path.endswith('.csv'):
            data = pd.read_csv(file_path, encoding='latin1')
        elif file_path.endswith('.xlsx'):
            data = pd.read_excel(file_path, encoding='latin1')
        else:
            raise ValueError("Unsupported file format. Please use .csv or .xlsx.")

        print(f"Dataset loaded successfully with {data.shape[0]} rows and {data.shape[1]} columns.")
        return data

    except FileNotFoundError:
        raise FileNotFoundError(f"Error: File not found at {file_path}.")
    except pd.errors.EmptyDataError:
        raise ValueError("Error: The file is empty.")
    except Exception as e:
        raise Exception(f"An unexpected error occurred loading data: {e}")


def highest_roi(data):
    """
    Calculate the top 10 cities with the highest mean Return on Investment (ROI).
    """
    try:
        # Calculate ROI: baseRent divided by totalRent (as a proxy for yield), * 100
        # Note: In real estate, ROI usually involves purchase price. 
        # Assuming this formula is per specific business requirement.
        data['ROI'] = (data['baseRent'] / data['totalRent']) * 100

        # Group by 'city', calculate the mean ROI, and sort descending
        top_roi_areas = data.groupby('city')['ROI'].mean().sort_values(ascending=False).head(10)

        return top_roi_areas
    except KeyError as e:
        print(f"Missing required column for ROI calculation: {e}")
        return None
    except Exception as e:
        print(f"An error occurred while calculating ROI: {e}")
        return None


def common_rental_price_range(data):
    """
    Determine the most common rental price range based on totalRent.
    """
    try:
        # Define rental price bins
        bins = [0, 500, 1000, 1500, 2000, 3000, 5000]
        labels = ['0-500', '501-1000', '1001-1500', '1501-2000', '2001-3000', '3001-5000']

        # Create a new column for the range
        data['rent_range'] = pd.cut(data['totalRent'], bins=bins, labels=labels)

        # Get the most common rental price range
        # dropna=True ensures we don't count missing values or out-of-bounds values
        most_common_range = data['rent_range'].value_counts(dropna=True).idxmax()
        return most_common_range
    except KeyError:
        print("Error: 'totalRent' column is missing in the dataset.")
        return None
    except Exception as e:
        print(f"An error occurred while calculating rental price range: {e}")
        return None


def top_rent_increases(data):
    """
    Identify the top 10 ZIP codes (geo_plz) with the highest average rent increase year-over-year.
    """
    try:
        # Create a 'year' column if it doesn't exist, using 'yearConstructed' as proxy
        if 'year' not in data.columns and 'yearConstructed' in data.columns:
            data['year'] = data['yearConstructed']

        # 1. Calculate average rent per ZIP code per Year
        avg_rent_per_zip_year = data.groupby(['geo_plz', 'year'])['totalRent'].mean()

        # 2. Calculate percent change. 
        # IMPORTANT: We must group by 'geo_plz' again before pct_change
        # to ensure we don't calculate the change between two different ZIP codes.
        rent_increase_series = avg_rent_per_zip_year.groupby(level='geo_plz').pct_change()

        # 3. Average the increases per ZIP code to find the consistently highest rising areas
        # dropna() removes the first year of data for each zip (which is NaN)
        avg_increase_per_zip = rent_increase_series.dropna().groupby('geo_plz').mean()

        top_zip_codes = avg_increase_per_zip.sort_values(ascending=False).head(10)
        return top_zip_codes
    except KeyError as e:
        print(f"Missing required column for rent increase calculation: {e}")
        return None
    except Exception as e:
        print(f"An error occurred while calculating rent increases: {e}")
        return None


def most_common_heating_type(data):
    """
    Find the most commonly used heating type.
    """
    try:
        common_heating = data['heatingType'].value_counts().idxmax()
        return common_heating
    except KeyError:
        print("Error: 'heatingType' column is missing in the dataset.")
        return None
    except Exception as e:
        print(f"An error occurred while determining the most common heating type: {e}")
        return None


def avg_rent_per_sqft_top_cities(data):
    """
    Calculate the average rent per square foot for the top 5 cities 
    (defined by the highest number of listings).
    """
    try:
        # Calculate rent per square unit (assuming meters or feet based on input data)
        data['rent_per_sqft'] = data['baseRent'] / data['livingSpace']

        # Identify top 5 cities by volume of listings
        top_cities = data['city'].value_counts().head(5).index

        # Filter data for these cities
        top_cities_data = data[data['city'].isin(top_cities)]

        # Calculate average rent per sqft for these cities
        avg_rent = top_cities_data.groupby('city')['rent_per_sqft'].mean().sort_values(ascending=False)

        return avg_rent
    except KeyError as e:
        print(f"Missing required column: {e}")
        return None
    except Exception as e:
        print(f"An error occurred while calculating rent per square foot: {e}")
        return None