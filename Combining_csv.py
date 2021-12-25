'''
Combines all the csv present
Author:-TITHI PATEL
        ARCHI SHAH
'''
from glob import glob
import pandas as pd
import os


user = glob('./*.csv')
get_datafile=[]
for data in user:
        # takes the file which have final in the end and as final list
        if data != './Final_.csv':
            get_datafile.append(data)
        else:
          print(data)
          os.remove("{}".format(data))

print(get_datafile)

for i in get_datafile:
  # checking the index and one having largest 1st row is replaced in final file
  if (i==get_datafile[0]):
    df1=pd.read_csv("{}".format(i))
    df1.set_index('Analysis', inplace = True)
    df1.to_csv("Final_.csv")
  else:
    df=pd.read_csv("Final_.csv")
    df1=pd.read_csv("{}".format(i))
    if len(df1) > len(df):
      result = pd.concat([df1, df.iloc[:,1:]], axis=1)
    else:
      result = pd.concat([df, df1.iloc[:,1]], axis=1)
    result.set_index('Analysis', inplace = True)
    result.to_csv("Final_.csv")

