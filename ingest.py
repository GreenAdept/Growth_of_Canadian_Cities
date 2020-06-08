import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import sys
from collections import OrderedDict

year = '2016'
filename_2016 = './2016/2016_92-151_XBB.txt'
filename_2011 = './2011/2011_92-151_XBB_XLSX.xlsx'
filename_2006 = './2006/2006_92-151-XBB_XLSX.xlsx'
#filename_2006 = './2006/test.xlsx'
filename_2001 = './2001/2001 - Census Tract Profiles.csv'
filename_1996 = './1996/1996 - Census Tract Profiles.csv'


def display_help():
    global year
    print('When running this program, please pass 1 argument to specify the year of census data to process.')
    year = input("Which census year would you like to process? ")


def read_attributes_file(filename, year, filetype='excel'):
    engine = create_engine('sqlite:///stats-{}.sqlite'.format(year), echo=False)

    with engine.connect() as connection:
        connection.execute('''CREATE TABLE IF NOT EXISTS Provinces
            (PRuid INTEGER PRIMARY KEY,
            PRename TEXT)''')

        connection.execute('''CREATE TABLE IF NOT EXISTS Electoral_Districts
            (FEDuid INTEGER PRIMARY KEY,
            FEDname TEXT,
            PRuid INTEGER,
            FOREIGN KEY(PRuid) references Provinces(PRuid))''')

        connection.execute('''CREATE TABLE IF NOT EXISTS Urban_Areas
            (CMAuid TEXT PRIMARY KEY,
            CMAname TEXT,
            CMAtype TEXT,
            SACtype TEXT,
            PRuid INTEGER,
            FOREIGN KEY(PRuid) references Provinces(PRuid))''')

        connection.execute('''CREATE TABLE IF NOT EXISTS Census_Divisions
            (CDuid INTEGER PRIMARY KEY,
            CDname TEXT,
            CDtype TEXT,
            PRuid INTEGER,
            FOREIGN KEY(PRuid) references Provinces(PRuid))''')

        connection.execute('''CREATE TABLE IF NOT EXISTS Census_Subdivisions
            (CSDuid REAL PRIMARY KEY,
            CSDname TEXT,
            CSDtype TEXT,
            CDuid INTEGER,
            FOREIGN KEY(CDuid) references Census_Divisions(CDuid))''')

        connection.execute('''CREATE TABLE IF NOT EXISTS Census_Tracts
            ('index' INTEGER PRIMARY KEY,
            CTuid TEXT,
            CMAuid TEXT,
            FOREIGN KEY(CMAuid) references Urban_Areas(CMAuid))''')

        connection.execute('''CREATE TABLE IF NOT EXISTS Population_Centres
            ('index' INTEGER PRIMARY KEY,
            POPCTRRAPuid INTEGER,
            POPCTRRAname TEXT,
            POPCTRRAtype INTEGER,
            POPCTRRAclass INTEGER,
            CMAuid INTEGER,
            FOREIGN KEY(CMAuid) references Urban_Areas(CMAuid))''')

        connection.execute('''CREATE TABLE IF NOT EXISTS Dissemination_Areas
            (DAuid REAL PRIMARY KEY,
            DARPlamx REAL,
            DARPlamy REAL,
            DARPlat REAL,
            DARPlong REAL,
            CSDuid REAL,
            FOREIGN KEY(CSDuid) references Census_Tracts(CSDuid))''')

        connection.execute('''CREATE TABLE IF NOT EXISTS Dissemination_Blocks
            (DBuid REAL PRIMARY KEY,
            DBpop INTEGER,
            DBarea REAL,
            DAuid INTEGER,
            FOREIGN KEY(DAuid) references Dissemination_Areas(DAuid))''')

    dtypes = OrderedDict({'DBuid': 'int',
                          'DBpop': 'Int64',
                          'DBtdwell': 'Int64',
                          'DBurdwell': 'Int64',
                          'DBarea': 'float',
                          'DBir': 'bool',
                          'DAuid': 'Int64',
                          'DArplamx': 'float',
                          'DArplamy': 'float',
                          'DArplat': 'float',
                          'DArplong': 'float',
                          'PRuid': 'int',
                          'PRname': 'str',
                          'PRename': 'str',
                          'PRfname': 'str',
                          'PReabbr': 'str',
                          'PRfabbr': 'str',
                          'FEDuid': 'int',
                          'FEDname': 'str',
                          'ERuid': 'int',
                          'ERname': 'str',
                          'CDuid': 'int',
                          'CDname': 'str',
                          'CDtype': 'str',
                          'ADAuid': 'int',
                          'ADAcode': 'int',
                          'CSDuid': 'str',
                          'CSDname': 'str',
                          'CSDtype': 'str',
                          'SACtype': 'int',
                          'SACcode': 'int',
                          'CCSuid': 'str',
                          'CCSname': 'str',
                          'DPLuid': 'str',
                          'DPLname': 'str',
                          'DPLtype': 'str',
                          'CMAPuid': 'str',
                          'CMAuid': 'str',
                          'CMAname': 'str',
                          'CMAtype': 'str',
                          'CTuid': 'str',
                          'CTcode': 'str',
                          'CTname': 'str',
                          'POPCTRRAPuid': 'str',
                          'POPCTRRAuid': 'str',
                          'POPCTRRAname': 'str',
                          'POPCTRRAtype': 'Int64',
                          'POPCTRRAclass': 'Int64'})
    # headers = ['DBuid', 'DBpop', 'DBtdwell', 'DBurdwell', 'DBarea', 'DBir', 'DAuid', 'DArplamx',
    #            'DArplamy', 'DArplat', 'DArplong', 'PRuid', 'PRname', 'PRename', 'PRfname', 'PReabbr', 'PRfabbr',
    #            'FEDuid', 'FEDname', 'ERuid', 'ERname', 'CDuid', 'CDname', 'CDtype', 'ADAuid', 'ADAcode', 'CSDuid',
    #            'CSDname', 'CSDtype', 'SACtype', 'SACcode', 'CCSuid', 'CCSname', 'DPLuid', 'DPLname', 'DPLtype',
    #            'CMAPuid', 'CMAuid', 'CMAname', 'CMAtype', 'CTuid', 'CTcode', 'CTname', 'POPCTRRAPuid', 'POPCTRRAuid',
    #            'POPCTRRAname', 'POPCTRRAtype', 'POPCTRRAclass']
    if year == '2011':
        dtypes.__delitem__('ADAuid')
        dtypes.__delitem__('ADAcode')
    if year == '2006':
        dtypes.__delitem__('ADAuid')
        dtypes.__delitem__('ADAcode')
        dtypes.__delitem__('CMAuid')
        dtypes.__delitem__('POPCTRRAuid')
        dtypes.__delitem__('POPCTRRAclass')

    if filetype == 'excel':
        df = pd.read_excel(filename, header=None, names=dtypes.keys(), dtype=dtypes)
        # header=0,
    elif filetype == 'text':
        df = pd.read_csv(filename, sep=',', engine='python', header=0, names=dtypes.keys(), dtype=dict(dtypes))

    if year == '2001':
        None
    elif year == '2006':
        df.insert(df.columns.get_loc('CMAPuid') + 1, 'CMAuid', df['CMAPuid'].apply(lambda x: x[2:]).astype('str'))
    elif year == '2016':
        # Left pad the Census Tract IDs
        df = df.astype({'CTuid': 'str'})
        df['CTuid'] = df['CTuid'].str.zfill(10)

    df[['DBuid', 'DBpop', 'DBarea', 'DAuid']].to_sql('Dissemination_Blocks', con=engine, if_exists='append',
                                                     index=False)
    df[['DAuid', 'DArplamx', 'DArplamy', 'DArplat', 'DArplong', 'CSDuid']].drop_duplicates().to_sql(
        'Dissemination_Areas', con=engine, if_exists='append', index=False)
    df[['CTuid', 'CMAuid']].drop_duplicates().to_sql('Census_Tracts', con=engine, if_exists='append')
    df[['CSDuid', 'CSDname', 'CSDtype', 'CDuid']].drop_duplicates().to_sql('Census_Subdivisions', con=engine,
                                                                           if_exists='append', index=False)
    df[['CDuid', 'CDname', 'CDtype', 'PRuid']].drop_duplicates().to_sql('Census_Divisions', con=engine,
                                                                        if_exists='append', index=False)
    df[['ERuid', 'ERname', 'PRuid']].drop_duplicates().to_sql('Economic_Regions', con=engine, if_exists='append',
                                                              index=False)
    df[['FEDuid', 'FEDname', 'PRuid']].drop_duplicates().to_sql('Electoral_Districts', con=engine, if_exists='append',
                                                                index=False)
    df[['PRuid', 'PRename']].drop_duplicates().to_sql('Provinces', con=engine, if_exists='append', index=False)
    df[['CMAuid', 'CMAname', 'CMAtype', 'SACtype', 'PRuid']].drop_duplicates().to_sql('Urban_Areas', con=engine,
                                                                                      if_exists='append', index=False)
    if year == '2006':
        df[['POPCTRRAPuid', 'POPCTRRAname', 'POPCTRRAtype', 'CMAuid']].drop_duplicates().to_sql('Population_Centres',
                                                                                                 con=engine,
                                                                                                 if_exists='append')
    else:
        df[['POPCTRRAPuid', 'POPCTRRAname', 'POPCTRRAtype', 'POPCTRRAclass', 'CMAPuid']].drop_duplicates().to_sql(
            'Population_Centres', con=engine, if_exists='append')


def read_census_tract_profiles(filename):
    df = pd.read_csv(filename, index_col=False, low_memory=False, encoding="ISO-8859-1")
    df = df.loc[(df['Profile of Cens'] == 'Population, 2001 - 100% Data') | (
            df['Profile of Cens'] == 'Land area in square kilometres, 2001')]
    df = df.set_index(pd.Index(['Population', 'Land Area']))
    df = df.drop(columns='Profile of Cens')
    df = df.transpose()

    engine = create_engine('sqlite:///stats-{}.sqlite'.format(year), echo=False)
    df[['CTuid', 'CMAPuid']].drop_duplicates().to_sql('Census_Tracts', con=engine, if_exists='append')
    df[['FEDuid', 'FEDname', 'PRuid']].drop_duplicates().to_sql('Electoral_Districts', con=engine, if_exists='append',
                                                                index=False)
    df[['PRuid', 'PRename']].drop_duplicates().to_sql('Provinces', con=engine, if_exists='append', index=False)
    df[['POPCTRRAPuid', 'POPCTRRAname', 'POPCTRRAtype', 'CMAPuid']].drop_duplicates().to_sql(
        'Population_Centres', con=engine, if_exists='append')


if len(sys.argv) < 2:
    if len(sys.argv) > 2 or sys.argv[1] == '-h':
        display_help()
year = sys.argv[1]

if year == '2016':
    read_attributes_file(filename_2016, year, filetype='text')
elif year == '2011':
    read_attributes_file(filename_2011, year)
elif year == '2006':
    read_attributes_file(filename_2006, year)
elif year == '2001':
    read_census_tract_profiles(filename_2001)
elif year == '1996':
    None
elif year == '1991':
    None
