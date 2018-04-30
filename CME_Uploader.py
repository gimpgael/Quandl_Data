# -*- coding: utf-8 -*-
"""
Tool to load the market information from Quandl into a SQL Database that I will
use, for the contracts I am interested by.
"""

# Import librairies
import os
import sys
import datetime
import quandl
import sqlite3
import pandas as pd

from pandas.tseries.offsets import *

class Uploader():
    """ Algorithm designed to create and then update the Quandl CME Database. 
    It connects to the platform with my credentials, and then retrieve and update
    the information.
    
    Attributes:
    -----------------  
    - api_key: Proprietary key to connect to Quandl
    - chemin: directory with the files and the database
    - dts: date from the one to start doing the update, by default today
    - db_name: Name of the database
    - years_forward: Number of years to look ahead to create the tickers, 
    by default 4
    """

    def __init__(self, 
                 api_key = "a5oa6Q4y8Na8D6rQ3ebd", 
                 chemin = "C:\\Users\\pages\\Desktop\\Work\\SystematicDesk\\",
                 dts = datetime.datetime.today(),
                 db_name = 'MarketData.db',
                 years_forward = 4):
        """Initialize algorithm"""
        
        # Initialize the correct directory
        os.chdir(chemin)
        
        self.api_key = api_key
        self.chemin = chemin
        self.dts = dts
        self.db_name = db_name
        
        # Initialise connection
        self.init_quandl()
        
        # Initialise contracts to look for
        self.contracts, self.df_dash = self.init_contracts(years_forward)
        
        
    def init_quandl(self):
        """Initialise the Quandl API key for all connections"""
        
        quandl.ApiConfig.api_key = self.api_key        
        
    def init_contracts(self, years_forward):
        """List the contracts to be studied, and the correspondance matrix"""
        
        def extract_root(x):
            """Intermediate function to extract the root ticker"""
            return x.split('/')[1]
            
        contracts_lst = []
        
        # Read the info from the main excel file
        df_dash = pd.read_excel(self.chemin + 'CMEGroup.xlsx', sheetname = 'dash', header = 0)
        df_quandl = pd.read_excel(self.chemin + 'CMEGroup.xlsx', sheetname = 'CMEGroup', header = None)
        
        # Create a column with the alias, for correspondance
        df_quandl[5] = df_quandl[4].apply(extract_root)
        
        # Loop for the contracts to retrieve
        for ctrc in df_dash['SYMBOL']:
            
            # Extract the intermediate dataframe for the months
            months_list = list(df_quandl.loc[df_quandl[5] == ctrc, 3].values[0])
            
            # Loop to generate contracts
            for yr in range(self.dts.year, self.dts.year + years_forward):
                for m in months_list:
                    contracts_lst.append(ctrc + m + str(yr))
                    
        # Return the list and the dataframe
        return contracts_lst, df_dash
    
    def quandl_extract(self, tick, dts_beg, dts_end):
        """Extract Quandl info, from a strating date to an ending date"""
        
        return quandl.get('CME/' + tick, start_date = dts_beg.strftime('%Y-%m-%d'), 
                          end_date = dts_end.strftime('%Y-%m-%d'))
    
    def create_database(self):
        """Create the database with market data"""
        
        # SQL queries to create tables
        table_root = """ CREATE TABLE IF NOT EXISTS RootCommodity (
                          id_root INTEGER PRIMARY KEY AUTOINCREMENT,
                          Name VARCHAR(30) NOT NULL,
                          Market VARCHAR(10) NOT NULL,
                          Alias VARCHAR(10) NOT NULL);"""

        table_contracts = """ CREATE TABLE IF NOT EXISTS Contract (
                          id_contract INTEGER PRIMARY KEY AUTOINCREMENT,
                          Month VARCHAR(1) NOT NULL,
                          Year INT NOT NULL,
                          id_root INT NOT NULL,
                          FOREIGN KEY (id_root) REFERENCES RootCommodity(id_root));"""
        
        table_date = """ CREATE TABLE IF NOT EXISTS Date (
                          id_date INTEGER PRIMARY KEY AUTOINCREMENT,
                          Date DATE NOT NULL);"""
                
        table_table = """CREATE TABLE IF NOT EXISTS contract_date (
                          Price FLOAT NOT NULL,
                          OpenInterest INT NOT NULL,
                          Volume INT NOT NULL,
                          id_date INT NOT NULL,
                          id_contract INT NOT NULL,
                          PRIMARY KEY (id_date, id_contract),
                          FOREIGN KEY (id_date) REFERENCES Date(id_date)
                          ON DELETE CASCADE ON UPDATE NO ACTION,
                          FOREIGN KEY (id_contract) REFERENCES Contract(id_contract)
                          ON DELETE CASCADE ON UPDATE NO ACTION);"""        
        
        # Connection
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        
        # Create tables
        c.execute(table_root)
        c.execute(table_contracts)
        c.execute(table_date)
        c.execute(table_table)
        
        # Database feeding
        # Dates
        date_dict = {}
        d0 = datetime.datetime.today()
        d1 = datetime.date(self.dts.year, self.dts.month, self.dts.day)
        d2 = datetime.date(d0.year, d0.month, d0.day)
        
        delta = d2 - d1
        
        for i in range(delta.days + 1):
            c.execute('INSERT INTO Date (Date) VALUES (?)', (d1 + datetime.timedelta(days = i),))
            date_dict[d1 + datetime.timedelta(days = i)] = c.lastrowid
            
        # Root commodities
        root_dict = {}
        
        for i in self.df_dash.index:
            c.execute('INSERT INTO RootCommodity (Name, Market, Alias) VALUES (?,?,?)', (self.df_dash.loc[i, 'NAME'],
                      self.df_dash.loc[i, 'EXCHANGE'], self.df_dash.loc[i, 'SYMBOL']))
            root_dict[self.df_dash.loc[i, 'SYMBOL']] = c.lastrowid
        
        # Contracts   
        for ctrc in self.contracts:
            
            id_root = root_dict[ctrc[:-5]]
            m, yr = ctrc[-5:-4], int(ctrc[-4:])
            
            c.execute('INSERT INTO Contract (Month, Year, id_root) VALUES (?,?,?)', (m, yr, id_root))
            id_contract = c.lastrowid
            
            # Extract Quandl data, test if request works
            try:
                df = self.quandl_extract(ctrc, d1, d2)
                
                # Loop through dates
                for i in df.index:
                    
                    # Extract info to put in the database
                    d, px, v, oi = self.extract_info(i, df)
                                 
                    c.execute('INSERT INTO contract_date (Price, OpenInterest, Volume, id_date, id_contract) VALUES (?,?,?,?,?)',
                              (px, oi, v, date_dict[d], id_contract))
                    
            except:
                pass
                
        # Save changes and close the connection
        conn.commit()
        conn.close()
        
    def get_last_date(self):
        """Select the last date saved in the database"""
        
        # Connection
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()     
        
        c.execute('SELECT MAX(Date) FROM Date')
        
        return datetime.datetime.strptime(c.fetchall()[0][0], '%Y-%m-%d').date()
    
    def get_contracts(self):
        """Select all contracts in the database"""
        
        def concat(x,y,z):
            return str(x) + str(y) + str(z)

        # Connection
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()         
        
        c.execute("""SELECT RootCommodity.Alias, Contract.Month, Contract.Year, Contract.id_contract,
                  FROM RootCommodity, Contract
                  WHERE RootCommodity.id_root = Contract.id_root;""")
        
        # Extract tickers and merge them
        df = pd.DataFrame(c.fetchall())
        df[4] = df[0] + df[1] + df[2].map(str)
        
        # Return a dictionnary
        return dict(zip(list(df[4]), list(df[3])))
    
    def get_roots(self):
        """Select all roots tickers"""
        
        # Connection
        conn = sqlite3.connect(u.db_name)
        c = conn.cursor()    

        c.execute("""SELECT Alias, id_root
                      FROM RootCommodity""")
        
        return dict(c.fetchall())
    
    def  extract_info(self, i, df):
        """Intermediate function extracting the data from a dataframe at a certain date"""
        
        d = i.date()
        px = df.loc[i, 'Settle']
        v = df.loc[i, 'Volume']
                    
        # The different cases of Open Interest
        if 'Previous Day Open Interest' in df.columns:
            oi = df.loc[i, 'Previous Day Open Interest']
        elif 'Prev. Day Open Interest' in df.columns:
            oi = df.loc[i, 'Prev. Day Open Interest']
        elif 'Open Interest' in df.columns:
            oi = df.loc[i, 'Open Interest']
        elif 'Prev Day Open Interest' in df.columns:
            oi = df.loc[i, 'Prev Day Open Interest']
        else:
            print('Open Interest column not found')
                        
        return d, px, v, oi
        
    def update_database(self):
        """Update the database"""
        
        # Test the last date versus the day to run the algo
        assert(self.get_last_date() < self.dts), 'The date is already in the Database'
        
        # Connection
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
                
        # Database feeding
        # Dates
        date_dict = {}
        d0 = datetime.datetime.today()
        d1 = datetime.date(self.dts.year, self.dts.month, self.dts.day)
        d2 = datetime.date(d0.year, d0.month, d0.day)
        
        delta = d2 - d1
        
        for i in range(delta.days + 1):
            c.execute('INSERT INTO Date (Date) VALUES (?)', (d1 + datetime.timedelta(days = i),))
            date_dict[d1 + datetime.timedelta(days = i)] = c.lastrowid        
        
        # Return dictionnaries of all contracts in the database and root commodities
        contract_dict = self.get_contracts()
        root_dict = self.get_roots()
        
        # Loop through contracts
        for ctrc in self.contracts:
            
            # If the contract is not already in the database, then add it
            if ctrc not in contract_dict.keys():
                id_root = root_dict[ctrc[:-5]]
                m, yr = ctrc[-5:-4], int(ctrc[-4:])
            
                c.execute('INSERT INTO Contract (Month, Year, id_root) VALUES (?,?,?)', (m, yr, id_root))
                contract_dict[ctrc] = c.lastrowid
        
            # Extract Quandl data, test if request works
            try:
                df = self.quandl_extract(ctrc, d1, d2)
                
                # Loop through dates
                for i in df.index:
                    
                    # Extract info to put in the database
                    d, px, v, oi = self.extract_info(i, df)
                                  
                    c.execute('INSERT INTO contract_date (Price, OpenInterest, Volume, id_date, id_contract) VALUES (?,?,?,?,?)',
                              (px, oi, v, date_dict[d], contract_dict[ctrc]))
                    
            except:
                pass
            
        # Save changes and close the connection
        conn.commit()
        conn.close()            
        
        