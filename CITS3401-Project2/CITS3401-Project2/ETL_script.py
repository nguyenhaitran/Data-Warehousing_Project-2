import pandas as pd

# Define the path and the dataset
file_path = 'data/'
dataset = ['crime.csv',
           'crime_25471_50000.csv',
           'crime_50001_75000.csv',
           'crime_75001_100000.csv',
           'crime_100001_125000.csv',
           'crime_125001_150000.csv',
           'crime_150001_175000.csv',
           'crime_175001_200000.csv',
           'crime_200001_225000.csv']


def process_data(file_path, dataset):
    """
    Read in dataset, process and then combine them into one dataframe

    Parameters
    ----------
    file_path : str
        The path of the dataset
    dataset : list
        The list of dataset

    Returns
    -------
    result_data : dataframe
    """

    data1 = pd.read_csv(file_path + dataset[0], low_memory=True)
    result_data = data1[['number', 'crime', 'date', 'beat', 'neighborhood', 'npu',
                         'type', 'road', 'city', 'county', 'state', 'country']]
    result_data = result_data.iloc[:25471]

    for data in dataset[1:]:
        tmp_data = pd.read_csv(file_path + data, low_memory=True)
        tmp_data = tmp_data[['number','crime', 'date', 'beat', 'neighborhood', 'npu',
                            'type', 'road', 'city', 'county', 'state', 'country']]

        for index, row in tmp_data.iterrows():
            for column in tmp_data.columns:
                # only strip white space for string columns
                if tmp_data[column].dtype == 'string':
                    tmp_data.at[index, column] = tmp_data.at[index,
                                                             column].strip()

        result_data = pd.concat([result_data, tmp_data], ignore_index=True)

    result_data = result_data.dropna().drop_duplicates()
    
    # rename crime column to crime_type
    result_data = result_data.rename(columns={'crime': 'crime_type'})

    # remove Sandy Springs from city column
    result_data = result_data[result_data['city'] != 'Sandy Springs']

    result_data['date'] = pd.to_datetime(result_data['date'])

    # remove Dekalb County from county column
    # for now, we do not remove Dekalb.
    # result_data = result_data[result_data['county'] != 'DeKalb County']

    return result_data


def severity(crime):
    """
    Catetorize crime type based on crime severity (Low, Medium, High)

    Parameters
    ----------
    crime : str
        The crime type

    Returns
    -------
    str
        The crime severity
    """

    if crime in ['LARCENY-NON VEHICLE', 'BURGLARY-NONRES',  'AUTO THEFT', 'LARCENY-FROM VEHICLE']:
        return 'Low'
    elif crime in ['BURGLARY-RESIDENCE', 'ROBBERY-COMMERCIAL', 'ROBBERY-RESIDENCE', 'ROBBERY-PEDESTRIAN']:
        return 'Medium'
    elif crime in ['AGG ASSAULT', 'RAPE', 'HOMICIDE']:
        return 'High'
    else:
        return 'Unknown'



def crime_nature(crime):
    """
    Catetorize crime type based on crime nature (violent or property)

    Parameters
    ----------
    crime : str
        The crime type

    Returns
    -------
    str
        The crime nature
    """
        
    if crime in ['RAPE', 'AGG ASSAULT', 'HOMICIDE', 'ROBBERY-RESIDENCE', 'ROBBERY-PEDESTRIAN', 'ROBBERY-COMMERCIAL']:
        return 'Violent'
    elif crime in ['LARCENY-NON VEHICLE', 'LARCENY-FROM VEHICLE', 'AUTO THEFT', 'BURGLARY-RESIDENCE', 'BURGLARY-NONRES']:
        return 'Property'
    else:
        return 'Unknown'
    


def crime(dataframe):
    """
    Create Crime.csv file from the dataframe
    Columns: cID, crime_type, severity, crime_nature

    Parameters
    ----------
    dataframe : dataframe
        The dataframe that contains the crime data

    Returns
    -------
    crime_table : dataframe
        The crime table
    """

    crime_table = dataframe[['crime_type']].copy()

    crime_table['severity'] = crime_table['crime_type'].apply(severity)

    # Map the crime types to their categories and add a new column to the crime dimension table
    crime_table['crime_nature'] = crime_table['crime_type'].apply(crime_nature)

    # As we remove duplicated based on number in the process_data function, therefore, we do not remove duplicate values in here.
    crime_table = crime_table.dropna().reset_index(drop=True)

    crime_table.insert(0, 'cID', range(1, 1 + len(crime_table)))
    crime_table['cID'] = crime_table['cID'].astype(int)
    crime_table.to_csv(
        'Crime.csv', header=True, index=False, sep=',', lineterminator='\n')

    return crime_table


def property(dataframe):
    """
    Create Property.csv file from the dataframe
    Columns: pID, type

    Parameters
    ----------
    dataframe : dataframe
        The dataframe that contains the crime data

    Returns
    -------
    property_type_table : dataframe
        The property type table
    """

    property_type_table = dataframe[["type"]]

    property_type_table = property_type_table.drop_duplicates().dropna().reset_index(drop=True)

    property_type_table.insert(0, 'pID', range(1, 1 + len(property_type_table)))
    
    property_type_table.to_csv('Property.csv', header=True, index=False, sep=',', lineterminator='\n')

    return property_type_table


def date(dataframe):
    """
    Create Date.csv file from the dataframe
    Columns: dID, date, month, quarter, year

    Parameters
    ----------
    dataframe : dataframe
        The dataframe that contains the crime data

    Returns
    -------
    date_table : dataframe
        The date table
    """

    date_table = dataframe[['date']].copy()
    date_table['date'] = pd.to_datetime(date_table['date'])
    date_table['month'] = date_table['date'].dt.month.astype(str) #we extract the month and we convert it to string
    date_table['quarter'] = date_table['month'].apply(
        lambda x: (int(x) - 1) // 3 + 1).astype(int)
    date_table['year'] = date_table['date'].dt.year.astype(str) #we extract the year and we convert it to string

    date_table = date_table.dropna().drop_duplicates().reset_index(drop=True)

    date_table.insert(0, 'dID', range(1, 1 + len(date_table)))

    date_table.to_csv('Date.csv', header=True, index=False, sep=',', lineterminator='\n')

    return date_table


def beat(dataframe):
    """
    Create Beat.csv file from the dataframe
    Columns: bID, beat, zone

    Parameters
    ----------
    dataframe : dataframe
        The dataframe that contains the crime data

    Returns
    -------
    beat_table : dataframe
        The beat table
    """

    beat_table = dataframe[['beat']].copy()
    beat_table['zone'] = beat_table['beat'].str[0:1]

    beat_table = beat_table.dropna().drop_duplicates().reset_index(drop=True)

    beat_table.insert(0, 'bID', range(1, 1 + len(beat_table)))

    beat_table.to_csv('Beat.csv', header=True, index=False, sep=',', lineterminator='\n')

    return beat_table


def location(dataframe):
    """
    Create Location.csv file from the dataframe
    Columns: lID, road, neighborhood, npu

    Parameters
    ----------
    dataframe : dataframe
        The dataframe that contains the crime data

    Returns
    -------
    location_table : dataframe
        The location table
    """

    location_table = dataframe[['road', 'neighborhood', 'npu']].copy()

    location_table = location_table.dropna().drop_duplicates().reset_index(drop=True)

    location_table.insert(0, 'lID', range(1, 1 + len(location_table)))

    location_table.to_csv('Location.csv', header=True, index=False, sep=',', lineterminator='\n')

    return location_table


def crime_property(dataframe, property_table):
    """
    Create Crime_Property.csv file from the dataframe
    Columns: cID, pID

    Parameters
    ----------
    dataframe : dataframe
        The dataframe that contains the crime data
    property_table : dataframe
        The dataframe that contains the property data

    Returns
    -------
    crime_property : dataframe
        The crime_property table
    """

    crime_property = dataframe.merge(property_table, how='left')
    crime_property.insert(0, 'cID', range(1, 1 + len(crime_property)))
    crime_property = crime_property[['cID', 'pID']]
    crime_property.to_csv(
        'Crime_Property.csv', columns=['cID', 'pID'], header=True, index=False, sep=',', lineterminator='\n')
    return crime_property


def crime_beat(dataframe, beat_table):
    """
    Create Crime_Beat.csv file from the dataframe
    Columns: cID, bID

    Parameters
    ----------
    dataframe : dataframe
        The dataframe that contains the crime data
    beat_table : dataframe
        The dataframe that contains the beat data

    Returns
    -------
    crime_beat : dataframe
        The crime_beat table
    """

    crime_beat = dataframe.merge(beat_table, how='left')
    crime_beat.insert(0, 'cID', range(1, 1 + len(crime_beat)))
    crime_beat = crime_beat[['cID', 'bID']]
    crime_beat = crime_beat.dropna()
    crime_beat['bID'] = crime_beat['bID'].astype(int)
    crime_beat.to_csv(
        'Crime_Beat.csv', columns=['cID', 'bID'], header=True, index=False, sep=',', lineterminator='\n')
    return crime_beat


def crime_date(dataframe, date_table):
    """
    Create Crime_Date.csv file from the dataframe
    Columns: cID, dID

    Parameters
    ----------
    dataframe : dataframe
        The dataframe that contains the crime data
    date_table : dataframe
        The dataframe that contains the date data

    Returns
    -------
    crime_date : dataframe
        The crime_date table
    """

    crime_date = dataframe.merge(date_table, how='left')
    crime_date.insert(0, 'cID', range(1, 1 + len(crime_date)))
    crime_date = crime_date[['cID', 'dID']]
    crime_date.to_csv(
        'Crime_Date.csv', columns=['cID', 'dID'], header=True, index=False, sep=',', lineterminator='\n')
    return crime_date


def crime_location(dataframe, location_table):
    """
    Create Crime_Location.csv file from the dataframe
    Columns: cID, lID

    Parameters
    ----------
    dataframe : dataframe
        The dataframe that contains the crime data
    location_table : dataframe
        The dataframe that contains the location data

    Returns
    -------
    crime_location : dataframe
        The crime_location table
    """

    crime_location = dataframe.merge(location_table, how='left')
    crime_location.insert(0, 'cID', range(1, 1 + len(crime_location)))
    crime_location = crime_location[['cID', 'lID']]
    crime_location.to_csv(
        'Crime_Location.csv', columns=['cID', 'lID'], header=True, index=False, sep=',', lineterminator='\n')
    return crime_location


# Create the dataframe using CSV files in the dataset
dataframe = process_data(file_path, dataset)

# Export CSV files for import nodes into Neo4j database
crime_table = crime(dataframe)
property_table = property(dataframe)
date_table = date(dataframe)
beat_table = beat(dataframe)
location_table = location(dataframe)

# Export CSV files for import relationships into Neo4j database
crime_property_table = crime_property(dataframe, property_table)
crime_beat_table = crime_beat(dataframe, beat_table)
crime_date_table = crime_date(dataframe, date_table)
crime_location_table = crime_location(dataframe, location_table)