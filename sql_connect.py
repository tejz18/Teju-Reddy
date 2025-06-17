import json
import pandas as pd
import pyodbc
from Transformation import*
from load import*

with open("sql.json")as config:
    Var1=json.load(config)
def sql_pull():
    #connection Strings
    connect_string=(f"Driver={Var1['Sql']['Driver']};"
     f"Server={Var1['Sql']['ServerName']};"
     f"Database={Var1['Sql']['Database']};"               
     f"UID={Var1['Sql']['UserName']};"
     f"PWD={Var1['Sql']['Password']};"
     )
    try:
        #establishing connection to a sql server
         connection=pyodbc.connect(connect_string)
         print("Connection successfull")
         return connection

    except pyodbc.Error as e:
        #handling the error
        print(f"error connecting to sql server:{e}")
        return None

def fetchdata(query):
  connection=sql_pull()
  if connection is not None:
      #you can do anything with the connection
      try:
          result=pd.read_sql(query,connection)
          print("Done Data Fetching")
          return result

      except Exception as e:
         print("error while running the query",e)
         return None

      finally:
           connection.close()
           print("connection closed")




''''def close_connection(connection):
    #closing connection
    if connection:
        connection.close()
        print("connection closed")'''

if __name__=="__main__":
    #function call
    #connection=sql_pull()
    query= "select * from dbo.students"
    result= fetchdata(query)
    if result is not None:
        print(result)
        transformed_data=transform(result)
    if transformed_data is not None:
        print(transformed_data)
        load(transformed_data)
    '''if connection:
       close_connection(connection)'''

