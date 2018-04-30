# -*- coding: utf-8 -*-
"""
Algorithm extracting market information from the database, and saving it into
csv files.
"""

# Import librairies
import os
import datetime
import sqlite3
import pandas as pd

class DB_to_Excel():
    """Algorithm to extract the market data from the Database, and put it into
    a matrix format into a csv file. This is because we used to work like that
    
    Attributes:
    -----------------  
    - chemin: directory with the files and the database
    - db_name: Name of the database 
    """
    
    def __init__(self,
                 chemin = "C:\\Users\\pages\\Desktop\\Work\\SystematicDesk\\",
                 db_name = 'MarketData.db'):
        """Initialize algorithm"""
        
        # Initialize the correct directory
        os.chdir(chemin)
        
        self.chemin = chemin
        self.db_name = db_name
        
    def request(self):
        """Extract price data from the Database, format it, fill some missing 
        values and save it into a csv format"""
        
        # Create data range and convert it into datetime
        dts_all = pd.date_range(start = '01/01/1990', end = '04/24/2018')
        dts_all = [x.date() for x in dts_all]
        
        # Connection
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        
        c.execute("""SELECT Date.Date, (RootCommodity.Alias || Contract.Month || Contract.Year) as Alias, contract_date.Price
                    FROM Date, RootCommodity, Contract, contract_date
                    WHERE Date.id_date = contract_date.id_date and
                    Contract.id_root = RootCommodity.id_root and
                    contract_date.id_contract = Contract.id_contract;""")
        
        # Extract the data in a DataFrame format
        df = pd.DataFrame(c.fetchall(), columns = ['Date', 'Alias', 'Value'])
        
        # Pivot it, in the matrix format I like, and convert the index
        df = df.pivot(index = 'Date', columns = 'Alias', values = 'Value')
        df.index = [datetime.datetime.strptime(x, '%Y-%m-%d') for x in df.index]
        
        # Reindex with correct dates
        df = df.reindex(index = dts_all)
        df = df.fillna(method = 'ffill', limit = 3, axis = 0)
        
        # Remove Saturdays and Sundays
        week_end = [x for x in df.index if x.weekday() >= 5]
        df = df.drop(week_end)
        
        # Save the information in a csv format
        df.to_csv(self.chemin + 'mx_px.csv')
        
        