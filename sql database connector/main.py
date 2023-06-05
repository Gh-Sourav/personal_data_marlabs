import sqlite3
import csv
conn=sqlite3.connect("testing.db")
cursor=conn.cursor()


rest_data=[]
with open(r"D:\archive\cereal.csv") as f:
    csv_obj = csv.reader(f)
    for row in csv_obj:
        # query = f"create table Nutration {tuple(row)}"
        # qury = f"Insert into Nutration Values {tuple(row)}"
        # qury = f"delete from Nutration where name='name'"
        qury = "select * from Nutration"
        data = cursor.execute(qury)
        for i in data:
            print(i)
conn.commit()
conn.close()

# all_values="""Insert into nutration (header) Values (row)"""
# count=cursor.execute(all_values)
# conn.commit()
# print("Record inserted")

