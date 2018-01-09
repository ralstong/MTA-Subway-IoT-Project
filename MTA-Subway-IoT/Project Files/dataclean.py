import pandas as pd
import numpy as np
from pandas import read_csv
import csv
with open('outfinal2.csv', 'a') as outcsv:
    writer = csv.writer(outcsv)
    writer.writerow(["tripId","timest","Day","Route","time96","time42"])
    with open('outtrial.csv', 'rb') as incsv:
        reader = csv.reader(incsv)
        writer.writerows(row for row in reader)
a = pd.read_csv('outfinal2.csv',index_col=False)
#data = a.groupby('tripId').apply(lambda x: x.ix[x.timest.idxmax()])
a = a.drop_duplicates(subset=['tripId','timest','time96','Route','Day'])

data = a[a.time96!='None']

for i,drow in data.iterrows():
        if int(drow.time96)<int(drow.timest):
		data.set_value(i,'time96','None')

data = data[data.time96!='None']
data = data[data.time42!='None']
#data = data[(int(data.time96)) >= (int(data.timest))]
data.to_csv('outfiltered.csv',index=False)

