# -*- coding: utf-8 -*-
"""
Tool to create and update the CFTC COT from the Legacy report in the database,
for all commodities and FX contracts we are interested by

There are three main parts:
    : The function part, containing little tools
    : The Uploader class, containing the main algorithm
    : The Main part, called when running the script. Once everything is set up
    the user just need to run this file and the database will be automatically 
    updated.
"""

# Import librairies
import os
import sys
import datetime
import quandl
import sqlite3
import pandas as pd

from pandas.tseries.offsets import *

####### FUNCTIONS #######
def construct_date_update():
    """Returns the previous Tuesday, for CFTC COT update purpose, to avoid 
    loading the full database each time"""
            
    # Offset variable
    decal = 0
            
    # While loop
    while (datetime.datetime.now() - decal * Day()).weekday() != 1:
        decal += 1
            
    dt = (datetime.datetime.now() - decal * Day()).date()
            
    # return in a string format, for Quandl
    return datetime.datetime.strftime(dt, '%Y-%m-%d') 
#########################

class Uploader():
    """Algortihm designed to update the CFTC COT information for the
    legacy report, from an external excel file. It connects to Quandl with my 
    settings, and retrieve the information.
    
    Attributes:
    -----------------  
    - api_key: Proprietary key to connect to Quandl
    - ticker_init: beginning of the ticker nomenclature
    - ticker_end: ending of the ticker nomenclature
    - file_dash: Excel file reference
    - db_name: Name of the database
    - chemin: directory with the files and the database
    - dts_update: The previous Tuesday, determined by a function
    """
    
    def __init__(self, api_key = "a5oa6Q4y8Na8D6rQ3ebd", 
                 ticker_init = "CFTC/",
                 ticker_end = "_FO_L_ALL",
                 file_dash = "CFTC_Dash.xlsx",
                 db_name = "COT.db",
                 chemin = "C:\\Users\\pages\\Desktop\\Work\\SystematicDesk\\",
                 dts_update = construct_date_update()):
        """Initialize algorithm"""
        
        # Initialize the correct directory
        os.chdir(chemin)
        
        # Initialise variables
        self.api_key = api_key
        self.ticker_init = ticker_init
        self.ticker_end = ticker_end,
        self.file_dash = file_dash,
        self.db_name = db_name
        self.dts_update = dts_update
        
        # Initialise connection
        self.init_quandl()
        
        # Initialise mapping table
        self.read_dash()
        
    def read_dash(self):
        """Read the dashboard file"""
        
        self.mapping = pd.read_excel(self.file_dash[0], header = 0, sheetname = 'dash')
        
    def init_quandl(self):
        """Initialise the Quandl API key for all connections"""
        
        quandl.ApiConfig.api_key = self.api_key
        
    def get_ongoing_data(self, ticker):
        """Call Quandl to get the last updated data"""
        
        # Construct Quandl ticker
        ticker = self.ticker_init + ticker.replace('#','').replace(' ','') + self.ticker_end[0]
        
        # Call the information of the ticker from the strating date to update
        return quandl.get(ticker, start_date = self.dts_update)
    
    def get_historical_data(self, ticker):
        """Call Quandl to load the full data history"""
        
        # Construct Quandl ticker
        ticker = self.ticker_init + ticker.replace('#','').replace(' ','') + self.ticker_end[0]
        
        # Call the information of the ticker
        return quandl.get(ticker)   
    
    def get_actors(self):
        """Return the list of actors we are interested by"""
        
        lst = ['Noncommercial', 'Commercial', 'Nonreportable']
        
        return lst

    def create_database(self):
        """Creates the SQL database"""
        
        # SQL queries to create tables
        table_date = """CREATE TABLE IF NOT EXISTS Date (
          id_date INTEGER PRIMARY KEY AUTOINCREMENT,
          Day INT NOT NULL,
          Month INT NOT NULL,
          Year INT NOT NULL);"""

        table_report = """CREATE TABLE IF NOT EXISTS Report (
          id_report INTEGER PRIMARY KEY AUTOINCREMENT,
          Type VARCHAR(255) NOT NULL);"""

        table_actor = """CREATE TABLE IF NOT EXISTS Actor (
          id_actor INTEGER PRIMARY KEY AUTOINCREMENT,
          Name VARCHAR(255) NOT NULL,
          id_report INT NOT NULL,
          FOREIGN KEY (id_report) REFERENCES Report(id_report));"""
        
        table_contract = """CREATE TABLE IF NOT EXISTS Contract (
          id_commo INTEGER PRIMARY KEY AUTOINCREMENT,
          Alias VARCHAR(5) NOT NULL,
          Commodity VARCHAR(20) NOT NULL,
          Market VARCHAR(10) NOT NULL);"""

        table_position = """CREATE TABLE IF NOT EXISTS Position (
          id_position INTEGER PRIMARY KEY AUTOINCREMENT,
          Value INT NOT NULL,
          Type VARCHAR(10) NOT NULL,
          Crop VARCHAR(5) NOT NULL,
          id_actor INT NOT NULL,
          id_date INT NOT NULL,
          id_commo INT NOT NULL,
          FOREIGN KEY (id_actor) REFERENCES Actor(id_actor),
          FOREIGN KEY (id_date) REFERENCES Date(id_date),
          FOREIGN KEY (id_commo) REFERENCES Contract(id_commo));"""
                
        # Connection
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        
        # Create tables
        c.execute(table_date)
        c.execute(table_report)
        c.execute(table_actor)
        c.execute(table_contract)
        c.execute(table_position)
        
        # ----- Database feeding -----  
        # Report type
        c.execute('INSERT INTO Report(Type) VALUES (?)', ('Legacy',))
        id_report = c.lastrowid
        
        # Actors: feed the databse and keep a dictionnary of IDs
        actors = self.get_actors()
        act_dict = {}
        
        for act in actors:
            c.execute('INSERT INTO Actor(Name, id_report) VALUES (?,?)', (act, id_report))
            act_dict[act] = c.lastrowid
        
        
        for i in self.mapping.index:
            tick = self.mapping.loc[i, 'code_cftc']
            alias = self.mapping.loc[i, 'abbreviation']
            commo = self.mapping.loc[i, 'commo']
            market = self.mapping.loc[i, 'market']
            
            # Extract the data from Quandl
            df = self.get_historical_data(tick)
        
            # Contract Studied
            c.execute('INSERT INTO Contract(Alias, Commodity, Market) VALUES (?,?,?)', (alias, commo, market))
            id_commo = c.lastrowid
        
            # Loops for dates and positions
            for dts in df.index:
                
                # Extract date characteristics
                d, m, y = self.extract_date(dts)
                
                c.execute('INSERT INTO Date (Day, Month, Year) VALUES (?,?,?)',
                          (d, m, y))
                id_date = c.lastrowid
                
                for var in df.columns:
                    
                    # Test if this is an actor we want to study
                    if var.split(' ')[0] in actors:
                        
                        pos_type = var.split(' ')[-1]
                        crop_type = 'All'
                        
                        c.execute('INSERT INTO Position(Value, Type, Crop, id_actor, id_date, id_commo) VALUES (?,?,?,?,?,?)',
                                  (df.loc[dts, var], pos_type, crop_type, act_dict[var.split(' ')[0]], id_date, id_commo))
                        
        # Save changes and close the connection
        conn.commit()
        conn.close()
        
    def update_database(self):
        """Function updating the SQL database"""
        
        # Connection
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        
        # Initial test to see if the date is already in the database
        tick_test = self.mapping.loc[0, 'code_cftc']
        df = self.get_ongoing_data(tick_test)
        dts = df.index
        
        # Query the previous date entered, to check if the date is already inputed
        res = self.check_exist(conn, dts)   
        try:
            assert len(res) == 0
        except AssertionError:
            sys.exit('The date is already in the database')
        
        # If the date is not in the database, then continue the process and update the
        # database
        d, m, y = dts.day[0], dts.month[0], dts.year[0]
        
        c.execute('INSERT INTO Date (Day, Month, Year) VALUES (?,?,?)',
                    (d, m, y))
        id_date = c.lastrowid
        
        # Get actors IDs
        act_dict = self.extract_actors_ids(conn)
        actors = list(act_dict.keys())
        
        for i in self.mapping.index:
            tick = self.mapping.loc[i, 'code_cftc']
            commo = self.mapping.loc[i, 'commo']
        
            # Get last value
            df = self.get_ongoing_data(tick)

            # Extract the commodity id
            id_commo = self.extract_commo_id(conn, commo)
        
            for var in df.columns:
                    
                # Test if this is an actor we want to study
                if var.split(' ')[0] in actors:
                        
                    pos_type = var.split(' ')[-1]
                    crop_type = 'All'
                        
                    c.execute('INSERT INTO Position(Value, Type, Crop, id_actor, id_date, id_commo) VALUES (?,?,?,?,?,?)',
                        (df.loc[dts, var], pos_type, crop_type, act_dict[var.split(' ')[0]], id_date, id_commo))
                        
        # Save changes and close the connection
        conn.commit()
        conn.close()
        
    def check_exist(self, conn, dts):
        """Check if the date has already an entry in the database"""
        
        c = conn.cursor()
        d = str(dts.day[0])
        m = str(dts.month[0])
        y = str(dts.year[0])
        
        c.execute("SELECT * FROM Date WHERE (Day = %s AND Month = %s AND Year = %s)" % (d,m,y))        
        return c.fetchall()
    
    def extract_actors_ids(self, conn):
        """Extract the actors IDs from the Legacy report"""
        
        # Query
        c = conn.cursor()
        c.execute("""SELECT Actor.id_actor, Actor.Name
                    FROM Report, Actor
                    WHERE Report.id_report = Actor.id_report AND Report.Type = 'Legacy' """)
        res = c.fetchall()
        
        # Initialise a dictionnary, for result
        act_dict = {}
        
        for e in res:
            act_dict[e[1]] = e[0]
            
        return act_dict
    
    def extract_commo_id(self, conn, commo):
        """Extract ID for a specific commodity"""
        
        # Query
        c = conn.cursor()
        c.execute("SELECT id_commo FROM Contract WHERE Commodity = '" + commo + "'")
        
        return c.fetchall()[0][0]
        
    def extract_date(self, x):
        """Returns the day, month and year of a date"""
        
        return x.day, x.month, x.year

####### MAIN #######
if __name__ == '__main__':
        
    uploader = Uploader()
    #uploader.update_database()
    

        
        