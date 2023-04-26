import pyodbc
import pandas as pd
import os

def q_ms_sql(q, IP_ext = "46"):
    """
    Decription:
        This function is a tool the run SQL queries from a SQL server.
    
    Usage:
        q_ms_SQL(q = "...", IP_ext = "...")
    
    Arguments:
        q ------------- The SQL query you would like to run in SQL
        IP_ext -------- The server extension. For Upclick purposes: 45, 46 or 47
      
    Details:
        q is usually an argument surrounded by "" and is needs to follow SQL syntax
        IP-ex is usually surrounded by "" as it is added to a string that will be ran in consol

    Return:
        dataset --------- The dataset as a pandas dataframe.
    """

    driver =  os.environ['MS_SQL_DRIVER']               # Calling SQL server
    user_name = os.environ['MS_SQL_USER_MANE_ELI']      # Username to the SQL server 
    password = os.environ['MS_SQL_PASSWORD_ELI']        # Password to SQL server
    server = f'192.168.1.{IP_ext}'                      # IP address to SQL server and extension input
    ms_sql_connexion = pyodbc.connect(f"Driver='{driver}';UID={user_name};PWD={password};SERVER={server}")
    cursor = ms_sql_connexion.cursor()
    cursor.execute(q)
    dataset = cursor.fetchall()
    dataset = pd.DataFrame(dataset)                             
    ms_sql_connexion.close()
    
    return dataset 
