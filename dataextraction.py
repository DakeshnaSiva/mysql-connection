import pandas as pd
import mysql.connector
from lxml import etree as ET
import json

def store_data_to_mysql(file_path, source_type, url=None, data=None):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Changepond@123",
        database="new_db"
    )
    file_path=r'D:\dakeshna\db.json'

    mycursor = mydb.cursor()
    if source_type == 'csv':
        data = pd.read_csv(file_path)
    elif source_type == 'excel':
        data = pd.read_excel(file_path)
    elif source_type == 'xml':
        tree = ET.parse(file_path)
        root = tree.getroot()
        columns = [elem.tag for elem in root[0]]
        data = [[elem.text for elem in row] for row in root]
        data = pd.DataFrame(data, columns=columns)
    elif source_type == 'api':
        with open(file_path, 'r') as f:
            data = json.load(f)
        data = pd.DataFrame(data)

    data.fillna(0, inplace=True)

    for index, row in data.iterrows():
        sql = "INSERT INTO datas ({}) VALUES ({})".format(','.join(data.columns.tolist()), ','.join(['%s']*len(data.columns)))
        values = tuple(row)
        mycursor.execute(sql, values)

    mydb.commit()
    mydb.close()

store_data_to_mysql(r'D:\dakeshna\db.json', 'api')
