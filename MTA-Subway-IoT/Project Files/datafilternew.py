import pandas as pd
import csv

df = pd.read_csv('outfiltered.csv',index_col=False)
timestarr = pd.unique(df[['timest']].values.ravel())
dfnew = pd.DataFrame(columns = ['timest','day','time96lcl','time96exp','switch'])
for t in timestarr:
	dftstlcl = df[(df.timest==t)&(df.Route=='local')]
	dftstexp = df[(df.timest==t)&(df.Route=='exp')]
	
	for indexlcl,rowlcl in dftstlcl.iterrows():
		for indexexp,rowexp in dftstexp.iterrows():
			if rowlcl['time96']<=rowexp['time96']:
				diff = int(rowexp['time96']) - int(rowlcl['time96'])
				sw = (diff + int(rowexp['time42']) - int(rowexp['time96']))<=int((rowlcl['time42'])-int(rowlcl['time96']))
				#dfnewrow = pd.DataFrame([[t,rowlcl['Day'],sw]],columns = ['timest','day','switch']) 
				#dfnewrow.append(dfnew,ignore_index=True)
				dfnew.loc[len(dfnew)]=[t,rowlcl['Day'],rowlcl['time96'],rowexp['time96'],sw]
			#	print 'hello'
print dftstlcl
print dfnew
dfnew.to_csv('outs3-3.csv',index=False)
