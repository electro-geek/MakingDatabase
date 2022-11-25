import pandas as pd
import os
import mysql.connector


mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="MritunjaySharma",
    database="Giraffe"
)

mycursor = mydb.cursor()
currDir = os.getcwd()
path = currDir + "/" + "individual_stocks_5yr"
list1 = os.listdir(path)
mycursor.execute('CREATE TABLE StockDataA(date_ date, open INT, high INT, low INT, close INT, volume INT, name VARCHAR(100))')

for i in list1:
    df = pd.read_csv(path+"/"+i)
    for i,row in df.iterrows():
        sql = "INSERT INTO StockDataA(date_,open,high,low,close,volume,name) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        val = (row['date'],row['open'], row['high'], row['low'], row['close'], row['volume'], row['Name'])
        mycursor.execute(sql, val)
        
mycursor.execute("ALTER TABLE StockDataA ORDER BY date_, name ASC;")

mydb.commit()