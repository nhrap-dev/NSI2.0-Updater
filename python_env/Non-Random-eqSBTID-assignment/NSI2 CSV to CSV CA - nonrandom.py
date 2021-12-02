'''
NiyamIT
COLIN LINDEMAN, GIS Developer
Proof of Concept - Load NSI csv file data, massage data, export csv
    that can be appended to sql server table.
For these databases only: AK, CA, HI, OR, WA, PR, VI, AS, GU, MP
Last Update:2020-2-14
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
xlsx_config = r"E:\Projects\NSI_20200200\data\NSI2.0_to_tsNsiGbs_eqSBTIDControl.xlsx"

#For HI
##inputCsvPath = r'E:\Projects\NSI_20200200\data\15\HI15.txt'
##database = 'HI'
##xlsx_config_sheet = "NSI2.0_HI_tsNsiGb"'

#For CA
#inputCsvPath = r'E:\Projects\NSI_20200200\data\06\CA06_coastal.txt'
inputCsvPath = r'E:\Projects\NSI_20200200\data\06\CA06.txt'
database = 'CA'
xlsx_config_sheet = "NSI2.0_CA_tsNsiGbs"

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
query_eqBldgTypeDisplayOrder= "SELECT [eqBldgType],\
                                      [DisplayOrder]\
                               FROM [Hazus_model].[dbo].[eqclBldgType]"
query_hzSqFtFactors= "SELECT [Occupancy],\
                             [SquareFootage]\
                      FROM [CDMS].[dbo].[hzSqftFactors]"                            
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
try:
    sql_conn = pyodbc.connect(connectString, autocommit=False)
    df_hzSqFtFactors= pd.read_sql(query_hzSqFtFactors, sql_conn)
except Exception as e:
    print(" exception: {}".format((e)))

#Read the input csv
    
#CA
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
# try with dtypes defined, reminder that input fields were removed to reduce data
df_Csv = pd.read_csv(inputCsvPath, delimiter=",", dtype=dtypeCA)

#HI
##df_Csv = pd.read_csv(inputCsvPath, delimiter=",")
    
#Set all columns to lowercase
columnList = df_Csv.columns
columnDict = {}
for column in columnList:
    columnDict[column] = column.lower()
df_Csv = df_Csv.rename(columns=columnDict)

#OUTPUTS
outputCsvPath = r'E:\Projects\NSI_20200200\data\CA06_20200413.csv'




'''
MODIFY THE DATA...
This assumes the input NSI data has the same schema as when this script was created.
'''
#Setup groups for sorting later (part of random assignment?)
def ListGroupFunction(eqSBTID):
    if eqSBTID in ('MH  '):
        return 1
    elif eqSBTID in ('W1  ','C3L ','URML'):
        return 2
    else:
        return 9
    return 
df_tsSOccupSbtPct['group'] = df_tsSOccupSbtPct.apply(lambda x: ListGroupFunction(x['eqSBTID']), axis=1)


#Set the occupancy field to have match the values in sql
def SOccTypeIdFunction(OccType):
    #Sample Inputs: RES1-1SNB; COM9; RES3A; etc Need to pad with spaces on the right to match sql server table.
    foo = OccType.split('-')[0]
    #add trailing whitespace
    return foo.ljust(5,' ')
# Add a new field and use the def above to calculate it...
df_Csv['occupancy5'] = df_Csv.apply(lambda x: SOccTypeIdFunction(x['occtype']), axis=1)


#Update AreaSqFt field via lookup to CDMS hz.sqftfactors...
df_hzSqFtFactorsDict = df_hzSqFtFactors.to_dict('records')
def hzSqFtFactorsFunction(sqft, occupancy5):
    if sqft == 0:
        for x in df_hzSqFtFactorsDict:
            if occupancy5 == x['Occupancy']:
                return x['SquareFootage']
    else:
        return sqft
df_Csv['sqft'] = df_Csv.apply(lambda x: hzSqFtFactorsFunction(x['sqft'], x['occupancy5']), axis=1)


#[NsiID] OccType + ID, this needs to be unique
    #ID from NSI2.0 IS NOT UNIQUE, there are multiples of every # but 70k with id= 0. ID is 0 and up, integer.
    #ID=1 are four rows with x,y ~100km apart from each other. It appears the ID sequence restarts three times.'
try:
    # #for HI
    # df_Csv['id'].astype('int')
    # df_Csv['objectid'].astype('int')
    # df_Csv['nsiid'] = df_Csv['objectid'].astype(str) + '_' + df_Csv['occtype'] + '_' + df_Csv['id'].astype(str).replace('\.0', '', regex=True)
    
    #for CA
    df_Csv['fid'].astype('int')
    df_Csv['fd_id'].astype('int')
    df_Csv['NsiID'] = df_Csv['fid'].astype(str) + '_' + df_Csv['occtype'] + '_' + df_Csv['fd_id'].astype(str).replace('\.0', '', regex=True)
except Exception as e:
    print("  exception: {}".format((e)))


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
    df_Csv['persqftavgval'] = 0
except Exception as e:
    print("  exception: {}".format((e)))
#[FirstFloorHt] Found_Ht
try:
    df_Csv.rename(columns={'found_ht': 'firstfloorht'}, inplace=True)
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
    df_Csv['valother'] = 0
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
    #[Hazus_model].[dbo].[ClSOccupancy]  #xx.dbo.[SOccTypeId]?  #[HI].[dbo].[tsSOccupSbtPct]?  #[Hazus_model].[dbo].[flSOccupIDBase]?
# Lookup SQL Table to get values (requires that occupancy be padded with trailing whitespace...
df_Csv = pd.merge(df_Csv, df_clOccupancy, left_on='occupancy5', right_on='Occupancy', how='left', suffixes=('_left', '_right'))
# Rename field to match final schema...
try:
    df_Csv.rename(columns={'SoccID': 'socctypeid'}, inplace=True)
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


#[EqDesignLevelId] Based on shapefiles Med_Yr_Blt and table 3.5, assign eqClDesignLevel.
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
df_Csv['eqdesignlevelid'] = df_Csv.apply(lambda x: YearBuiltEqDesignLevelFunction(state, x['medyrblt']), axis=1)

# Add a new field and calculate it
query_eqClDesignLevel = "SELECT \
            [DesignLevelID], \
            [DesignLevel],\
            [DesignLevelDesc]\
            FROM [Hazus_model].[dbo].[eqClDesignLevel]"
df_eqClDesignLevel = pd.read_sql(query_eqClDesignLevel, sql_conn)
eqClDesignLevelDict = df_eqClDesignLevel.to_dict('records')
def eqbldgtypeFunction(DesignLevelID):
    for item in eqClDesignLevelDict:
        if DesignLevelID == item['DesignLevelID']:
            return item['DesignLevel']
    else:
        return '-9'
df_Csv['designlevel'] = df_Csv.apply(lambda x: eqbldgtypeFunction(x['eqdesignlevelid']), axis=1)


#[EqBldgTypeId]
# Create new field and normalize data in that field...
df_Csv['occtype2'] = np.where(df_Csv.occtype.str.match('RES1-.*'), 'RES1', df_Csv.occtype)

#Create a new field with a specific string to replace...
df_Csv['eqsbtid'] = 'NULL'

#eqSBTID non-random assignment:
#load config file...
df_config = pd.read_excel(xlsx_config, sheet_name=xlsx_config_sheet)
#Get a unique list of the values in the SOccupID field to iterate on...
SOccupIDList = df_config.SOccupID.unique().tolist()
#Iterate over the list of SOccupID and get values to assign eqSBTID...
for x in sorted(SOccupIDList):
    #filter dataframe by SOccupID and sort by RunFirst column...
    xdf = df_config[df_config['SOccupID'] == x].sort_values(by=['RunFirst'])
    for index, row in xdf.iterrows():
        
#        whereSOccupID = "(df_Csv['occupancy5'] == '" + row['SOccupID'] + "')"
#        
#        #check if a field is null using pd...
#        areaMin = row['areaMin']
#        if(pd.isnull(areaMin)):
#            areaMin = '0' 
#        areaMax = row['areaMax']
#        if(pd.isnull(areaMax)):
#            areaMax = '10000000'
#    
#        whereArea = "(df_Csv['areasqft'] >= " + str(areaMin) + ") & (df_Csv['areasqft'] <= " + str(areaMax) + ")"
#            
#       
#        nstoriesMin = row['nstoriesMin']
#        if(pd.isnull(nstoriesMin)):
#            nstoriesMin = '0'
#        nstoriesMax = row['nstoriesMax']
#        if(pd.isnull(nstoriesMax)):
#            nstoriesMax = '200'
#            
#        whereNstories = "(df_Csv['nstories'] >= " + str(nstoriesMin) + ") & (df_Csv['nstories'] <= " + str(nstoriesMax) + ")"
#        
#        
#        designLevel = row['DesignLevelMax']
#        whereDesignLevel = "(df_Csv['designLevel'] <= " + str(designLevel) + ")"
#        
#        
#        bldgType = row['bldgtype']
#        whereBldgType = "(df_Csv['bldgtype'] == '" + bldgType + "')"
#        
#        if(pd.isnull(designLevel)):
#            whereClause = ' & '.join([whereSOccupID, whereArea, whereNstories, whereBldgType])
#        else:
#            whereClause = ' & '.join([whereSOccupID, whereArea, whereNstories, whereDesignLevel, whereBldgType])
#        
#        eqSBTID = row['eqSBTID']
#        
#        #Set eqSBTID value...
#        df_Csv.loc[whereClause, 'eqsbtid'] = eqSBTID

        
        # other route (this works!)
        SOccupID = row['SOccupID']
        
        areaMin = row['areaMin']
        if(pd.isnull(areaMin)):
            areaMin = 0
            
        areaMax = row['areaMax']
        if(pd.isnull(areaMax)):
            areaMax = 100000000
            
        nstoriesMin = row['nstoriesMin']
        if(pd.isnull(nstoriesMin)):
            nstoriesMin = 0
            
        nstoriesMax = row['nstoriesMax']
        if(pd.isnull(nstoriesMax)):
            nstoriesMax = 1000
            
        designLevel = row['DesignLevelMax'] 
        
        bldgType = row['bldgtype']  
        
        eqSBTID = row['eqSBTID']
        
        #Set eqSBTID value...
        if(pd.isnull(designLevel)):
           df_Csv.loc[(df_Csv['eqsbtid'] == 'NULL') & \
                   (df_Csv['occupancy5'] == SOccupID) & \
                   (df_Csv['areasqft'] >= areaMin) & \
                   (df_Csv['areasqft'] <= areaMax) & \
                   (df_Csv['nstories'] >= nstoriesMin) & \
                   (df_Csv['nstories'] <= nstoriesMax) & \
                   (df_Csv['bldgtype'] == bldgType), 'eqsbtid'] = eqSBTID
        else:
            df_Csv.loc[(df_Csv['eqsbtid'] == 'NULL') & \
                   (df_Csv['occupancy5'] == SOccupID) & \
                   (df_Csv['areasqft'] >= areaMin) & \
                   (df_Csv['areasqft'] <= areaMax) & \
                   (df_Csv['nstories'] >= nstoriesMin) & \
                   (df_Csv['nstories'] <= nstoriesMax) & \
                   (df_Csv['eqdesignlevelid'] <= designLevel) & \
                   (df_Csv['bldgtype'] == bldgType), 'eqsbtid'] = eqSBTID

#For eqSTBTID that are still NULL, assign defaults...
df_Csv.loc[(df_Csv['eqsbtid'] == 'NULL') & \
            (df_Csv['bldgtype'] == 'C'), 'eqsbtid'] = 'C1L'
           
df_Csv.loc[(df_Csv['eqsbtid'] == 'NULL') & \
            (df_Csv['bldgtype'] == 'M'), 'eqsbtid'] = 'RM2L'
           
df_Csv.loc[(df_Csv['eqsbtid'] == 'NULL') & \
            (df_Csv['bldgtype'] == 'S'), 'eqsbtid'] = 'S3'
           
df_Csv.loc[(df_Csv['eqsbtid'] == 'NULL') & \
            (df_Csv['bldgtype'] == 'H'), 'eqsbtid'] = 'MH'
           
df_Csv.loc[(df_Csv['eqsbtid'] == 'NULL') & \
            (df_Csv['bldgtype'] == 'W') & \
            (df_Csv['areasqft'] < 5000 ), 'eqsbtid'] = 'W1'
           
df_Csv.loc[(df_Csv['eqsbtid'] == 'NULL') & \
            (df_Csv['bldgtype'] == 'W') & \
            (df_Csv['areasqft'] >= 5000.000001 ), 'eqsbtid'] = 'W2'


#eqBldgType in Hazus Model has trailing spaces to four characters (ie 'S3  ')...
eqBldgTypeDisplayOrderDict = df_eqBldgTypeDisplayOrder.to_dict('records')

def eqbldgtypeidFunction(eqBldgType):
    eqBldgType = eqBldgType.ljust(4,' ')
    for item in eqBldgTypeDisplayOrderDict:
        if eqBldgType == item['eqBldgType']:
            return item['DisplayOrder']
    else:
        return -9
    
df_Csv['eqbldgtypeid'] = df_Csv.apply(lambda x: eqbldgtypeidFunction(x['eqsbtid']), axis=1)


#Post Process
#Special Rule 1
#def PostProcessW1Function(state, eqBldgType, eqdesignlevelid):
#    if state == 'CA':
#        if eqBldgType == 'W1':
#            if eqdesignlevelid in (1, 2):
#                return 3
#    else:
#        if eqBldgType == 'W1':
#            if eqdesignlevelid == 1:
#                return 2
#df_Csv.apply(lambda x: PostProcessW1Function(state, x['bldgtype'], x['eqdesignlevelid']), axis=1)

#Special Rule 1
print('eqbldgtypeid=1 and eqdesignlevelid = 1', len(df_Csv[(df_Csv['eqbldgtypeid'] == 1) & (df_Csv['eqdesignlevelid'] == 1)]))
print('eqbldgtypeid=1 and eqdesignlevelid = 2', len(df_Csv[(df_Csv['eqbldgtypeid'] == 1) & (df_Csv['eqdesignlevelid'] == 2)]))

if state == 'CA':
    df_Csv.loc[(df_Csv['eqbldgtypeid'] == 1) & ((df_Csv['eqdesignlevelid'] == 1)|(df_Csv['eqdesignlevelid'] == 2)), ['eqdesignlevelid']] = 3
else:
    df_Csv.loc[(df_Csv['eqbldgtypeid'] == 1) & (df_Csv['eqdesignlevelid'] == 1), ['eqdesignlevelid']] = 2
    
print('Post Rule: eqbldgtypeid=1 and eqdesignlevelid = 1', len(df_Csv[(df_Csv['eqbldgtypeid'] == 1) & (df_Csv['eqdesignlevelid'] == 1)]))
print('Post Rule: eqbldgtypeid=1 and eqdesignlevelid = 2', len(df_Csv[(df_Csv['eqbldgtypeid'] == 1) & (df_Csv['eqdesignlevelid'] == 2)]))


#Drop fields that don't exist in the tsNsiGbs target table...
#CA
#try:
#    df_Csv.drop(['fid', 'id','occtype', 'yrbuilt','resunits',\
#                  'stacked','source','empnum','teachers','students',\
#                  'st_damcat','basement','bldgtype','found_type','fipsentry',\
#                  'firmzone','o65disable','u65disable','occupancy5'\], axis=1, inplace=True)
#except Exception as e:
#    print("  exception: {}".format((e)))
#HI
#try:
#    df_Csv.drop(['objectid', 'id','occtype', 'yrbuilt','resunits',\
#                  'stacked','source','empnum','teachers','students',\
#                  'st_damcat','basement','bldgtype','found_type','fipsentry',\
#                  'firmzone','o65disable','u65disable',\
#                  'valother', 'Occupancy','mhweightareasqft', 'w1weightareasqft',\
#                  'urmweighteqcldesignlevel', 'occtype2','eqsbtid'], axis=1, inplace=True)
#except Exception as e:
#    print("  exception: {}".format((e)))
    
#TEST
#Filter fields so that only the desired outpuf fields exist...
#df_Csv = df_Csv.filter(items=['nsiid','eqbldgtypeid','eqdesignlevelid',\
#                              'socctypeid','foundtypeid','cbfips','nstories',\
#                              'areasqft','persqftavgval','firstfloorht',\
#                              'valstruct','valcont','valother','valvehic',\
#                              'medyrblt','pop2pmu65','pop2pmo65','pop2amu65',\
#                              'pop2amo65','latitude','longitude'])
    
    
###Check what fields remain...
##outputColumns = sorted(df_Csv.columns)
##for column in outputColumns:
##    print column
#df_Csv.dtypes
#df_Csv.info()



#NA values will fail the conversion to int.
##CBFips needs to be pandas object
#df_Csv['cbfips'].astype('object')
##Pop2pmU65 needs to be pandas int64
#df_Csv['pop2pmu65'].astype('int')
##Pop2pmO65 needs to be pandas int64
#df_Csv['pop2pmo65'].astype('int')
##Pop2amU65 needs to be pandas int64
#df_Csv['pop2amu65'].astype('int')
##Pop2amO65 needs to be pandas int64
#df_Csv['pop2amo65'].astype('int')
##NStories needs to be pandas int64
#df_Csv['nstories'].astype('int')
##EqBldgTypeId needs to be pandas int64
#df_Csv['eqbldgtypeid'].astype('int')
##PerSqftAvgVal needs to be pandas float64
#df_Csv['persqftavgval'].astype('int')
##ValOther needs to be pandas float64
#df_Csv['valother'].astype('float')



''' 
EXPORT TO CSV FILE
'''

#Export to csv...
df_Csv.to_csv(outputCsvPath)



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
