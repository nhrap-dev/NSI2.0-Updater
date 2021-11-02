'''
NiyamIT
COLIN LINDEMAN, GIS Developer
Proof of Concept - Load NSI csv file data, massage data, export csv
    that can be appended to sql server table.
For these databases only: AK, CA, HI, OR, WA, PR, VI, AS, GU, MP
Last Update:2020-2-12
Created: 2020-1-28
Requirements:
   Python 3.7, pyodbc, Pandas (anaconda 64bit install)
   SQL Server 12.0.4237
'''

# Import necessary modules
import pyodbc
import pandas as pd
import numpy as np


#GET THE DATA...
#INPUTS
##inputCsvPath = 'C:/temp/15/HI15.txt'
##database = 'HI'
inputCsvPath = 'C:/temp/06/CA06.txt'
database = 'CA'
state = database

#SQL Server variables
serverName = 'SABRE-PC\HAZUSPLUSSRVR'
#Remove Username and Password before sharing!!
userName = 'hazuspuser'
password = 'Gohazusplus_02'
connectString = "Driver={SQL Server};Server="+serverName+\
                    ";Database="+database+";UID="+userName+";PWD="+password

#NOTE there might be a trailing space in the eqSBTID values, at least for HI.
query_tsSOccupSbtPct = "SELECT [SchemeID],\
                                                            [SOccupID],\
                                                            [eqSBTID],\
                                                            [Pct]\
                                              FROM ["+database+"].[dbo].[tsSOccupSbtPct]"
query_clOccupancy = "SELECT [Occupancy],\
                                                      [SoccID]\
                                        FROM [Hazus_model].[dbo].[clOccupancy]"
query_eqBldgTypeDisplayOrder= "SELECT [eqBldgType]\
                                                          ,[DisplayOrder]\
                                                          FROM [Hazus_model].[dbo].[eqclBldgType]"
#Connect to SQL Server, run the query and load query results into a pandas dataframe...
try:
    sql_conn = pyodbc.connect(connectString, autocommit=False)
    df_tsSOccupSbtPct = pd.read_sql(query_tsSOccupSbtPct, sql_conn)
except Exception as e:
    print(" exception: {}".format((e)))
try:
    sql_conn = pyodbc.connect(connectString, autocommit=False)
    df_clOccupancy = pd.read_sql(query_clOccupancy, sql_conn)
except Exception as e:
    print(" exception: {}".format((e)))
try:
    sql_conn = pyodbc.connect(connectString, autocommit=False)
    df_eqBldgTypeDisplayOrder= pd.read_sql(query_eqBldgTypeDisplayOrder, sql_conn)
except Exception as e:
    print(" exception: {}".format((e)))

#Read the input csv
dtypeCA = {'fid':'int64',\
                   'fd_id':'int64',\
                   'x':'float32',\
                   'y':'float32',\
                   'cbfips':'int64',\
                   'occtype':'object',\
                   'yrbuilt':'int64',\
                   'num_story':'int64',\
                   'sqft':'float64',\
                   'pop2amu65':'int64',\
                   'pop2amo65':'int64',\
                   'pop2pmu65':'int64',\
                   'pop2pmo65':'int64',\
                   'basement':'int64',\
                   'bldgtype':'object',\
                   'found_ht':'int64',\
                   'found_type':'object',\
                   'val_struct':'float64',\
                   'val_cont':'float64',\
                   'val_vehic':'float64',\
                   'med_yr_blt':'int64'}
df_Csv = pd.read_csv(inputCsvPath, delimiter=",", dtype=dtypeCA)
#try with dtypes defined, reminder that input fields were removed to reduce data
#df_Csv = pd.read_csv(inputCsvPath, delimiter=",", dtype=dtypeCA)

#OUTPUTS
outputCsvPath = 'C:/temp/CA06Test.csv'

'''
MODIFY THE DATA...
This assumes the input NSI data has the same schema as when this script was created.
'''
#[NsiID] OccType + ID, this needs to be unique
try:
    #for CA
    df_Csv['fid'].astype('int')
    df_Csv['fd_id'].astype('int')
    df_Csv['nsiid'] = df_Csv['fid'].astype(str) + '_' + df_Csv['occtype'] + '_' + df_Csv['fd_id'].astype(str).replace('\.0', '', regex=True)
except Exception as e:
    print("  exception: {}".format((e)))
    #ID IS NOT UNIQUE, there are multiples of every # but 70k with id= 0. ID is 0 and up, integer.
    #ID=1 are four rows with x,y ~100km apart from each other. It appears the ID sequence restarts three times.
    #NEED TO MAKE THIS UNIQUE PER ROW. Added objectid to beginning to make it unique and split values with underbar.
    #Casting the field as int did not appear to remove the '.0' from the value when it was converted to string.

#[CBFips] CBFips
    #Nothing to do here.
#[NStories] Num_Story
try:
    df_Csv.rename(columns={'num_story': 'nstories'}, inplace=True)
except Exception as e:
    print("  exception: {}".format((e)))
#[AreaSqft] SqFt
try:
    df_Csv.rename(columns={'sqft': 'areasqft'}, inplace=True)
except Exception as e:
    print("  exception: {}".format((e)))
#[PerSqftAvgVal] 0
try:
    df_Csv['PerSqftAvgVal'] = 0
except Exception as e:
    print("  exception: {}".format((e)))
#[FirstFloorHt] Found_Ht
try:
    df_Csv.rename(columns={'found_Ht': 'firstfloorht'}, inplace=True)
except Exception as e:
    print("  exception: {}".format((e)))
#[ValStruct] Val_Struct
try:
    df_Csv.rename(columns={'val_struct': 'valstruct'}, inplace=True)
except Exception as e:
    print("  exception: {}".format((e)))
#[ValCont] Val_Cont
try:
    df_Csv.rename(columns={'val_cont': 'valcont'}, inplace=True)
except Exception as e:
    print("  exception: {}".format((e)))
#[ValOther] 0
try:
    df_Csv['ValOther'] = 0
except Exception as e:
    print("  exception: {}".format((e)))
#[ValVehic] Val_Vehic
try:
    df_Csv.rename(columns={'val_vehic': 'valvehic'}, inplace=True)
except Exception as e:
    print("  exception: {}".format((e)))
#[MedYrBlt] Med_Yr_Blt
try:
    df_Csv.rename(columns={'med_yr_blt': 'medyrblt'}, inplace=True)
except Exception as e:
    print("  exception: {}".format((e)))
#[Pop2pmU65] Pop2pmU65
    #Nothing to do here.
#[Pop2pmO65] Pop2pmO65
    #Nothing to do here.
#[Pop2amU65] Pop2amU65
    #Nothing to do here.
#[Pop2amO65] Pop2amO65
    #Nothing to do here.
#[Latitude] Y
try:
    df_Csv.rename(columns={'y': 'latitude'}, inplace=True)
except Exception as e:
    print("  exception: {}".format((e)))
#[Longitude] X
try:
    df_Csv.rename(columns={'x': 'longitude'}, inplace=True)
except Exception as e:
    print("  exception: {}".format((e)))

#[SOccTypeId] (OccType left 4 or 5 from OccType created based on SOoccID in Hazus_model_data.clOccupancy)
    #[Hazus_model].[dbo].[ClSOccupancy]
        #xx.dbo.[SOccTypeId]?
        #[HI].[dbo].[tsSOccupSbtPct]?
        #[Hazus_model].[dbo].[flSOccupIDBase]?
def SOccTypeIdFunction(OccType):
    #Sample Inputs: RES1-1SNB; COM9; RES3A; etc Need to pad with spaces on the right to match sql server table.
    foo = OccType.split('-')[0]
    return foo.ljust(5,' ')
# Add a new field and use the def above to calculate it...
df_Csv['occupancy'] = df_Csv.apply(lambda x: SOccTypeIdFunction(x['occtype']), axis=1)
# Lookup SQL Table to get values...
df_Csv = pd.merge(df_Csv, df_clOccupancy, left_on='occupancy', right_on='Occupancy', how='left', suffixes=('_left', '_right'))
# Rename field to match final schema...
try:
    df_Csv.rename(columns={'soccid': 'socctypeid'}, inplace=True)
except Exception as e:
    print("  exception: {}".format((e)))

#[FoundTypeId] (Based on Found_Type and BasementID in Hazus_model_data.flFoundationType, watch for truncation (first 4 letters only) in NSI 2.0 data)
    #Hazus_model_data.flFoundationType
    #Does this need to be dynamic? Could look up values from table, then check to see if input is in any values (ie "Craw" in "Crawl Space")
def BasementIDFunction(Found_Type):
    if Found_Type == 'Pile':
        #Pile
        code = 1
    elif Found_Type == 'Pier':
        #Pier
        code = 2
    elif Found_Type == 'Soli':
        #Solid Wall
        code = 3
    elif Found_Type == 'Base':
        #Basement/Garden
        code = 4
    elif Found_Type == 'Craw':
        #Crawl Space
        code = 5
    elif Found_Type == 'Fill':
        #Fill
        code = 6
    elif Found_Type == 'Slab':
        #Slab on Grade
        code = 7
    else:
        code = 0
    return code
# Add a new field and use the def above to calculate it...
df_Csv['foundtypeid'] = df_Csv.apply(lambda x: BasementIDFunction(x['found_type']), axis=1)

#[EqBldgTypeId] Do this last.
###PLACEHOLDER
##try:
##    df_Csv['EqBldgTypeId'] = -9999
##except Exception as e:
##    print "  exception: {}".format((e))
    
#Find total count of records per x field
#Get percentage for y field
#Determine the count of records from x that is percent defined for y
#For each type in field y
    #Select percent random records where y column is null, assign y value
#Start with smallest percent then iterate towards largest. That way if any are left unassigned then it could become part of the largest.
tsSOccupSbtPctList = []
tsSOccupList = df_tsSOccupSbtPct['SOccupID'].unique().tolist()
#Populate tsSOccupSbtPctList list...
for x in sorted(tsSOccupList):
    whereClause = "SOccupID == '" + x + "'"
    y = df_tsSOccupSbtPct.query(whereClause).to_dict('records')
    tsSOccupSbtPctList.append([x,y])
# Create new field and normalize data in that field...
df_Csv['occtype2'] = np.where(df_Csv.occtype.str.match('RES1-.*'), 'RES1',df_Csv.occtype)
#Update tsSOccupSbtPctList to add number of rows to sample for each eqSBTID...
for x in tsSOccupSbtPctList:    
    for y in x[1]:
        #from the dictionary
        OccType = x[0]
        Pct = y['Pct']
        #from a query get total record count
        SOccupID = OccType.rstrip()
        df_X = df_Csv[df_Csv.occtype2 == SOccupID]
        numRecords = len(df_X)
        #convert to hundreths and multiple by record count
        smpCnt = (Pct/100) * numRecords
        #round normal, the percent number of records
        smpCntRndDown = np.round((Pct / 100) * numRecords, 0)
        y['smpCnt'] = smpCntRndDown
#Create a new field with a specific string to replace...
df_Csv['eqsbtid'] = 'NULL'
for x in tsSOccupSbtPctList:   
    OccType = x[0]
    print(OccType)
    # Sort by Pct values, with largest at the end...
    sortedList = sorted(x[1], key = lambda i: i['Pct'])
    smallerPctList = sortedList[0:-1]
    biggestPctList = sortedList[-1]

    #Some OccType only have one dictionary, i.e. MH for HI...
    if len(x[1]) > 1:
        #Assign all eqSBTID from smallerPctList...
        for y in smallerPctList:
            #from the dictionary
            eqSBTID = y['eqSBTID']
            smpCnt = int(y['smpCnt'])
            #query on OccType and eqSBTID is null
            SOccupID = OccType.rstrip()
            #Create a sample using the smpCnt
            df_Sample = df_Csv.loc[(df_Csv.occtype2 == SOccupID) & (df_Csv.eqsbtid =='NULL')].sample(n=smpCnt, replace=False)
            #Get a list of the samples' id's
            SampleIDs = df_Sample.index.values.tolist()
            #Modify those sample id's in the main dataframe
            for ID in SampleIDs:
                df_Csv.at[ID, 'eqsbtid'] = eqSBTID
            print(eqSBTID, len(SampleIDs))
            #print SampleIDs
        
    #Assign eqSBTID from biggestPctList which is just one item as a dictionary...
    #from the dictionary...
    eqSBTID = biggestPctList['eqSBTID']
    #query on OccType and eqSBTID is null
    SOccupID = OccType.rstrip()
    #Set all remaining null values in eqSBTID to this eqSBTID...
    df_Csv.loc[(df_Csv['occtype2'] == SOccupID) & (df_Csv['eqsbtid'] =='NULL'), 'eqsbtid'] = eqSBTID
    print(eqSBTID)
    print()
    
#eqBldgType in Hazus Model has trailing spaces to four characters (ie 'S3  ')...
def eqSBTIDFourCharFunction(eqSBTID):
    #Sample Inputs: S3, PC1, etc Need to pad with spaces on the right to match sql server table.
    return eqSBTID.ljust(4,' ')
# Add a new field and use the def above to calculate it...
df_Csv['eqsbtidfourchar'] = df_Csv.apply(lambda x: eqSBTIDFourCharFunction(x['eqsbtid']), axis=1)

#Lookup eqSBTID to EqBldgTypeId as integer value and apply to EqBldgTypeId field...
df_Csv = pd.merge(df_Csv, df_eqBldgTypeDisplayOrder, left_on='eqsbtidfourchar', right_on='eqBldgType', how='left', suffixes=('_left', '_right'))
#Rename DisplayOrder to EqBldgTypeId...
try:
    df_Csv.rename(columns={'DisplayOrder': 'eqbldgtypeid'}, inplace=True)
except Exception as e:
    print("  exception: {}".format((e)))
    
#EqBldgTypeId Check...
#W1 in CA coastal counties will be at least MC(3) (no PC(1) or LC(2) W1, per EQ Technical Manual) and LC(2) (no PC(1) W1) in other States.
#Note special W1 rules and need to run a post check on EQ building types since URM MC(3), HC(4) or HS(7) is not allowed, 
        #those need to be reassigned to RM1 and C3 changes to C1 when MC(3), HC(4) or HS(7)


#[EqDesignLevelId] Based on shapefiles Med_Yr_Blt and table 3.5, assign eqClDesignLevel. NEED TO ADD code for blank/null values
    #[Hazus_model].[dbo].[eqClDesignLevel]
def YearBuiltEqDesignLevelFunction(state, year):
    if state == 'AK':
        if year <= 1964:
            #code = 'PC'
            code = 1
        elif year >= 1965 and year <= 1994:
            #code =  'LC'
            code = 2
        elif year >= 1995 and year <= 2000:
            #code =  'MC'
            code = 3
        elif year >= 2001:
            #code = 'HC'
            code = 4
    elif state == 'CA':
        if year <= 1940:
            #code = 'PC'
            code = 1
        elif year >= 1941 and year<= 1975:
            #code =  'LC'
            code  = 2
        elif year >= 1976 and year <= 1994:
            #code =  'MC'
            code = 3
        elif year >= 1995 and year <= 2000:
            #code = 'HC'
            code = 4
        elif year >= 2001:
            #code = 'HS'
            code = 7
    elif state == 'HI':
        if year <= 1974:
            #code = 'PC'
            code = 1
        elif year >= 1975 and year <= 1994:
            #code =  'LC'
            code = 2
        elif year >= 1995 and year <= 2000:
            #code =  'MC'
            code = 3
        elif year >= 2001:
            #code = 'HC'
            code = 4
    elif state == 'OR':
        if year <= 1974:
            #code = 'PC'
            code = 1
        elif year >= 1975 and year <= 1994:
            #code =  'LC'
            code = 2
        elif year >= 1995 and year <= 2000:
            #code =  'MC'
            code = 3
        elif year >= 2001:
            #code = 'HC'
            code = 4
    elif state == 'WA':
        if year <= 1955:
            #code = 'PC'
            code = 1
        elif year >= 1956 and year <= 1974:
            #code =  'LC'
            code = 2
        elif year >= 1975 and year <= 2003:
            #code =  'MC'
            code = 3
        elif year >= 2004:
            #code = 'HC'
            code = 4
    elif state == 'PR':
        if year <= 1974:
            #code = 'PC'
            code = 1
        elif year >= 1975 and year <= 1994:
            #code =  'LC'
             code =2
        elif year >= 1995 and year <= 2005:
            #code =  'MC'
            code = 3
        elif year >= 2006:
            #code = 'HC'
            code = 4
    elif state == 'VI':
        if year <= 1974:
            #code = 'PC'
            code = 1
        elif year >= 1975 and year <= 2005:
            #code =  'LC'
            code =2
        elif year >= 2006:
            #code =  'MC'
            code = 3
    elif state == 'MP':
        if year <= 1974:
            #code = 'PC'
            code = 1
        elif year >= 1975 and year <= 2005:
           #code =  'LC'
             code =2
        elif year >= 2006:
            #code =  'MC'
             code =3
    elif state == 'GU':
        if year <= 1974:
            #code = 'PC'
            code = 1
        elif year >= 1975 and year <= 2005:
            #code =  'LC'
            code = 2
        elif year >= 2006:
            #code =  'MC'
            code = 3
    elif state == 'AS':
        if year <= 1974:
            #code = 'PC'
            code = 1
        elif year >= 1975 and year <= 2005:
            #code =  'LC'
            code = 2
        elif year >= 2006:
            #code =  'MC'
            code = 3
    # State does not match any of the above...
    else:
        code = 0
    return code
# Add a new field and use the def above to calculate it...
df_Csv['eqdesignlevelid'] = df_Csv.apply(lambda x: YearBuiltEqDesignLevelFunction(state, x['yrbuilt']), axis=1)


#Drop fields that don't exist in the tsNsiGbs target table...
try:
    df_Csv.drop(['fid', 'fd_id','OccType', 'yrbuilt','resunits',\
                  'stacked','source','empnum','teachers','students',\
                  'st_damcat','basement','bldgtype','found_type','fipsentry',\
                  'firmzone','O65disable','u65disable','occupancy'], axis=1, inplace=True)
except Exception as e:
    print("  exception: {}".format((e)))
    
###Check what fields remain...
##outputColumns = sorted(df_Csv.columns)
##for column in outputColumns:
##    print column
#df_Csv.dtypes
#df_Csv.info()

# Target Schema
'''
NsiID	nvarchar(24)
EqBldgTypeId	smallint
EqDesignLevelId	smallint
SOccTypeId	smallint
FoundTypeId	smallint
CBFips	nvarchar(15)
NStories	int
AreaSqft	numeric(38, 8)
PerSqftAvgVal	numeric(38, 20)
FirstFloorHt	numeric(38, 8)
ValStruct	numeric(38, 8)
ValCont	numeric(38, 8)
ValOther	numeric(38, 8)
ValVehic	numeric(38, 8)
MedYrBlt	int
Pop2pmU65	int
Pop2pmO65	int
Pop2amU65	int
Pop2amO65	int
Latitude	numeric(38, 8)
Longitude	numeric(38, 8)'''

#NA values will fail the conversion to int.
#CBFips needs to be pandas object
df_Csv['cbfips'].astype('object')
#Pop2pmU65 needs to be pandas int64
df_Csv['pop2pmu65'].astype('int')
#Pop2pmO65 needs to be pandas int64
df_Csv['pop2pmo65'].astype('int')
#Pop2amU65 needs to be pandas int64
df_Csv['pop2amu65'].astype('int')
#Pop2amO65 needs to be pandas int64
df_Csv['pop2amo65'].astype('int')
#NStories needs to be pandas int64
df_Csv['nstories'].astype('int')
#EqBldgTypeId needs to be pandas int64
df_Csv['eqbldgtypeid'].astype('int')
#PerSqftAvgVal needs to be pandas float64
df_Csv['persqftavgval'].astype('int')
#ValOther needs to be pandas float64
df_Csv['valother'].astype('float')

#Export to csv...
df_Csv.to_csv(outputCsvPath)


###create a dataframe for display of data
##df_Stats = pd.DataFrame(columns=['OccType',\
##                                 'numRec',\
##                                 'eqSBTID',\
##                                 'Pct',\
##                                 'smpCntRnd',\
##                                 'rsltCnt'])
