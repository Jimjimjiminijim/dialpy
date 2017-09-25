#! python3

import re
import os
import glob
import numpy as np
from pandas import Series,DataFrame
import pandas as pd
import psycopg2
import pyproj
from datetime import datetime as dt

#change path to parameter

opt='overwrite'

headerList=[]
#os.chdir(os.path.join('D:\\','GIS_work','ventavon','headers'))
os.chdir(os.path.join('D:\\','GIS_work','berre','headers'))
try:
    #conn = psycopg2.connect(dbname='dialtest', port='5432', user='postgres',
    #                        password='P0temkin', host='localhost')
    conn = psycopg2.connect(dbname='dialtest', port='5432', user='postgres',
                            password='Darkside1', host='localhost')
except:
    print("Can't connect to database")
cur = conn.cursor()


#scan files for location information

for filename in glob.glob('*.scanDat'):
	print( filename )
	f=open( filename,'r')

	line=f.readline()
	regex=re.compile("Software Version ")
	match=re.search(regex,line)
	if match:
		print('regex :: Software Version :: OK')
		#print(match)
		#print(match.groups())
	else:
		print('Not a header file or format wrong')
		f.close()
		exit(-1)

	lines = f.readlines()
	f.close()


	headerItem={}
	#using ([0-9]+ in the regex returns all the digits of the scan number)    
	regex=re.compile("Scan Number ([0-9]+)")
	#This only returns the first digit if the scan number    
	#regex=re.compile("Scan Number (\d)")
	for i, line in enumerate(lines):
		match=re.search(regex,line)
		if match:
			print ('regex :: Scan Number :: OK')
			##print ('Found on line %s: %s' % (i+1, match.groups()))
			##print('Scan Number is ', int(match.groups()[0]))
			headerItem['id']=int(match.groups()[0]) 
			break
	if match:
		print('Scan Number (ID) set to', int(match.groups()[0]))
	else:
		print('Not a valid file no scan id')
		exit(-1)


	searchtxt='GPW_W, Alt,ID, GPS_N,Site Name,Location_name, Description, Dial_heading, Numeric'
	for i, line in enumerate(lines):    
		if searchtxt in line and i+2 < len(lines):
			match=re.search(regex,lines[i+2])
			
			headerItem['GPS_W'],headerItem['Alt'],headerItem['ID'],headerItem['GPS_N'],headerItem['SiteName'],headerItem['LocationName'], headerItem['Description'], headerItem['Heading'],n = lines[i+2].split(',')
							
			break
	headerItem['FileName']=filename
	
    
	regex=re.compile("Scan Start: ")
	for i, line in enumerate(lines):
		match=re.search(regex,line)
		if match:

			print ('regex :: Scan Start :: OK')

			#convert time string into datetime object
			StartTime_datetime=dt.strptime(line[12:31], '%d/%m/%Y %H:%M:%S')
			#headerItem['StartTime']=line[12:31]
            
            
			#print(int(match.groups()[0]))
			headerItem['StartTime']=StartTime_datetime
			#headerItem['StartTime']=int(match.groups()[0]) 
			break
	if match:
		print('Scan Start Time is ', headerItem['StartTime'])
	else:
		print('Not a valid file no scan id')
		exit(-1)

    
	headerList.append(headerItem)
	
	
print(headerList)

headerArr = np.array(headerList)
print(headerArr.shape)
headerFrame = DataFrame(headerList)
print(headerFrame)


#cur.execute("SELECT ST_SetSRID(ST_MakePoint(%s, %s, %s),4326);", (x, y, z))


cur.execute("""SELECT datname FROM  pg_database""")
rows=cur.fetchall()


print ("\databases:\n")
for row in rows:
    print ("   ", row[0])

print('existing data')
cur.execute("SELECT * FROM public.tbl_locations;")
rows=cur.fetchall()
for row in rows:
    print(row[0])

locations=[]

for Hrow in headerArr:
    #print(Hrow)
    print(Hrow['LocationName'])
    if Hrow['LocationName'].lstrip(' ') in locations:
        print()
    else:
        locations.append(Hrow['LocationName'].lstrip(' '))
		
#to do - loop over data frame and create new database entries with points from gps info + name
print(locations)



#work on dataframe version
headerFrame = headerFrame.apply( lambda x: x.map(lambda y:  y.lstrip() if type(y)==str else y) )

headerFrame['GPS_N']=pd.to_numeric(headerFrame['GPS_N'])
headerFrame['GPS_W']=pd.to_numeric(headerFrame['GPS_W'])
headerFrame['Heading']=pd.to_numeric(headerFrame['Heading'])

#try out some pandas analysis

print(headerFrame.head(n=10))
byLocation = headerFrame.groupby('LocationName')
print(byLocation)
print(byLocation['GPS_W'].describe())
byLocation['GPS_N'].describe()
#byLocation['GPS_W'].mean()

headerFrame['LocationName'].unique()

def funcG(x):
    GPS_N=x['GPS_N'].mean()
    GPS_W=x['GPS_W'].mean()
    Heading=x['Heading'].mean()
    SiteName=x['SiteName'].iloc[0]
    Description=x['Description'].iloc[0]
	
    return Series([Description, GPS_N,GPS_W, Heading, SiteName],index=['Description','GPS_N', 'GPS_W','Heading','SiteName'])
locations=(byLocation.apply(funcG))
print(locations)

#cur.execute("DELETE FROM tbl_locations;");
#conn.commit()

for index,location in locations.iterrows():
    print(index)
    #check if location name exists
    
    cur.execute("SELECT name FROM tbl_locations WHERE name='{0}' AND site='{1}';".format(index,location['SiteName']))
    idcheck=cur.fetchone()
    #if exists and opt = overwrite then overwrite
    
    if idcheck != None:
        print('already exists - updating value')
        if opt=='overwrite':
            cur.execute("UPDATE tbl_locations SET (description, site,heading,elevation, the_geom) = ('{0}', '{1}' , '{2}' ,2 , ST_GeomFromEWKT('SRID=4326; POINT({3} {4})')) WHERE name = '{5}' AND site = '{1}';".format( location['Description'],location['SiteName'],location['Heading'],location['GPS_W'],location['GPS_N'],index))
	    #if exists and ott = add only then do nothing
    else:
        #if doesn't exist then add
        print('adding value')
        cur.execute("INSERT INTO tbl_locations(name,description, site,heading,elevation, the_geom) VALUES ('{0}', '{1}' , '{2}' , {3},1 , ST_GeomFromEWKT('SRID=4326; POINT({4} {5})'));".format(index, location['Description'],location['SiteName'],location['Heading'],location['GPS_W'],location['GPS_N']))

	
	#cur.execute("INSERT INTO tbl_locations(id,description, site,heading,elevation, the_geom) VALUES ('VM01','test','Ventavon',0.0,0,ST_GeomFromEWKT('SRID=4326; POINT(5.919001 44.350971 )'));")

conn.commit()


#fill or update full headers table






# tidy up
os.chdir(os.path.join('D:\\','GIS_work'))
cur.close()
conn.close()

