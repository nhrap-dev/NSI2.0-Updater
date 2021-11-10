"""
TODO:
[ ] Convert remainder processing into functions
[ ] Check for and handle/normalize data input fields, i.e. occtype values
[ ] Create main script separate from this one, to run the tool
[ ] Add stats for input data and output data to compare, i.e. occtype counts
"""

from datetime import timedelta
import numpy as np
import os
import pyodbc
import pandas as pd
import re
import time

class sqlTable:
    def __init__(self, state='', outputDir=''):
        '''
            Keyword Arguments:
                state: string -- the state db name (WA, AS, etc)
                outputDir: string -- not used
            
            Note:
                The Hazus_model database must be attached to SQL Server and when
                you are finished it must have is permissions set so EVERYONE can edit;
                it is usually located at 'C:\Program Files (x86)\Hazus\Data\Aggregation'
        '''
        self.state = state
        self.outputDir = outputDir
        computer_name = os.environ['COMPUTERNAME']
        serverName = f"{computer_name}\\HAZUSPLUSSRVR"
        driver = '{SQL Server}'
        userName = 'hazuspuser'
        password = 'Gohazusplus_02'
        connectString = f"Driver={driver};Server={serverName};Database={self.state};UID={userName};PWD={password}"
        try:
            self.sql_conn = pyodbc.connect(connectString, autocommit=False)
        except Exception as e:
            print(" exception: {}".format((e)))

    def get_tsSOccupSbtPct(self):
        '''
        Returns:
                df_tsSOccupSbtPct: pandas dataframe

        Notes:
            This is from the state database
        '''
        query = "SELECT [SchemeID],[SOccupID],[eqSBTID],[Pct]\
                FROM ["+self.state+"].[dbo].[tsSOccupSbtPct]"
        outputPath = os.path.join(self.outputDir, f'{self.state}_tsSOccupSbtPct.csv')
        try:
            df_tsSOccupSbtPct = pd.read_sql(query, self.sql_conn)
            df_tsSOccupSbtPct.to_csv(outputPath, index=False)
            return df_tsSOccupSbtPct
        except Exception as e:
            print(" exception: {}".format((e)))

    def get_clOccupancy(self):
        '''
        Returns:
                df_clOccupancy: pandas dataframe
        '''
        query = "SELECT [Occupancy],[SoccID]\
                FROM [Hazus_model].[dbo].[clOccupancy]"
        outputPath = os.path.join(self.outputDir, 'clOccupancy.csv')
        try:
            df_clOccupancy = pd.read_sql(query, self.sql_conn)
            df_clOccupancy.to_csv(outputPath, index=False)
            return df_clOccupancy
        except Exception as e:
            print(" exception: {}".format((e)))

    def get_eqBldgTypeDisplayOrder(self):
        '''
        Returns:
                df_eqBldgTypeDisplayOrder: pandas dataframe
        '''
        query = "SELECT [eqBldgType],[DisplayOrder]\
                FROM [Hazus_model].[dbo].[eqclBldgType]"
        outputPath = os.path.join(self.outputDir, 'eqBldgTypeDisplayOrder.csv')
        try:
            df_eqBldgTypeDisplayOrder = pd.read_sql(query, self.sql_conn)
            df_eqBldgTypeDisplayOrder.to_csv(outputPath, index=False)
            return df_eqBldgTypeDisplayOrder
        except Exception as e:
            print(" exception: {}".format((e)))

    def get_hzSqFtFactors(self):
        '''
        Returns:
                df_hzSqFtFactors: pandas dataframe
        '''
        query = "SELECT [Occupancy],[SquareFootage]\
                FROM [CDMS].[dbo].[hzSqftFactors]" 
        outputPath = os.path.join(self.outputDir, 'hzSqftFactors.csv')    
        try:
            df_hzSqFtFactors = pd.read_sql(query, self.sql_conn)
            df_hzSqFtFactors.to_csv(outputPath, index=False)
            return df_hzSqFtFactors
        except Exception as e:
            print(" exception: {}".format((e)))


class NSI:
    def __init__(self, state='', inputDir='', outputDir='', NSIFileName='', OutFileName=''):
        '''
        Keyword Arguments:
                state: string -- the state db name (WA, AS, etc)
                inputDir: string -- location of input tables
                outputDir: string -- location to write output to
                NSIFileName: string -- name of output file

        Returns:
            NSIFile: csv --  a csv file of the processed data in the output dir
            
        '''
        self.state = state
        self.inputDir = inputDir
        self.outputDir = outputDir
        self.NSIPath = os.path.join(self.inputDir, NSIFileName)
        self.OutFileName = OutFileName
        self.NSICsvOutputPath = os.path.join(self.outputDir, self.OutFileName)

        df_Csv = pd.read_csv(self.NSIPath, delimiter=",")
            
        #Set all columns to lowercase
        columnList = df_Csv.columns
        columnDict = {}
        for column in columnList:
            columnDict[column] = column.lower()
        df_Csv = df_Csv.rename(columns=columnDict)

        tsSOccupSbtPctPath = os.path.join(self.inputDir, f'{self.state}_tsSOccupSbtPct.csv')
        hzSqFtFactorsPath = os.path.join(self.inputDir, 'hzSqftFactors.csv') 
        clOccupancyPath = os.path.join(self.inputDir, 'clOccupancy.csv')
        eqBldgTypeDisplayOrderPath = os.path.join(self.inputDir, 'eqBldgTypeDisplayOrder.csv')

        df_tsSOccupSbtPct = pd.read_csv(tsSOccupSbtPctPath, delimiter=",")
        self.df_hzSqFtFactors = pd.read_csv(hzSqFtFactorsPath, delimiter=",")
        df_clOccupancy = pd.read_csv(clOccupancyPath, delimiter=",")
        df_eqBldgTypeDisplayOrder = pd.read_csv(eqBldgTypeDisplayOrderPath, delimiter=",")

        '''PROCESS DATA:'''
        df_tsSOccupSbtPct['group'] = df_tsSOccupSbtPct.apply(lambda x: self.ListGroupFunction(x['eqSBTID']), axis=1)

        # Add a new field and use the SOccTypeIdFunction above to calculate it...
        df_Csv['occupancy5'] = df_Csv.apply(lambda x: self.SOccTypeIdFunction(x['occtype']), axis=1)
        # Update AreaSqFt field via lookup to CDMS hz.sqftfactors...
        df_Csv['sqft'] = 0 #FOR RI add default value
        df_Csv['sqft'] = df_Csv.apply(lambda x: self.hzSqFtFactorsFunction(x['sqft'], x['occupancy5']), axis=1)
        
        try:
            #for RI
            df_Csv['objectid'].astype('int')
            df_Csv['nsiid'] = df_Csv['objectid'].astype(str) + '_' + df_Csv['occtype'].astype(str).replace('\.0', '', regex=True)

            #[NsiID] OccType + ID, this needs to be unique
            #ID from NSI2.0 IS NOT UNIQUE, there are multiples of every # but 70k with id= 0. ID is 0 and up, integer.
            #ID=1 are four rows with x,y ~100km apart from each other. It appears the ID sequence restarts three times.'
            # #for HI
            # df_Csv['id'].astype('int')
            # df_Csv['objectid'].astype('int')
            # df_Csv['nsiid'] = df_Csv['objectid'].astype(str) + '_' + df_Csv['occtype'] + '_' + df_Csv['id'].astype(str).replace('\.0', '', regex=True)
            # #for CA
            # df_Csv['fid'].astype('int')
            # df_Csv['fd_id'].astype('int')
            # df_Csv['NsiID'] = df_Csv['fid'].astype(str) + '_' + df_Csv['occtype'] + '_' + df_Csv['fd_id'].astype(str).replace('\.0', '', regex=True)
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

        # Add a new field and use the BasementIDFunction to calculate it...
        df_Csv['foundtypeid'] = df_Csv.apply(lambda x: self.BasementIDFunction(x['found_type']), axis=1)

        # Add a new field and use the YearBuiltEqDesignLevelFunction to calculate it...
        df_Csv['yrbuilt'] = 0 #for RI
        df_Csv['eqdesignlevelid'] = df_Csv.apply(lambda x: self.YearBuiltEqDesignLevelFunction(state, x['yrbuilt']), axis=1)

        #[EqBldgTypeId]
        #Set weighted fields
        df_Csv['mhweightareasqft'] = np.where(df_Csv['areasqft'] <= 1500, 0.99, 0.01)
        df_Csv['w1weightareasqft'] = np.where(df_Csv['areasqft'] <= 2000, 0.99, 0.01)
        df_Csv['urmweighteqcldesignlevel'] = df_Csv.apply(lambda x: self.urmweighteqcldesignlevelFunction(x['eqdesignlevelid']), axis=1)

        #Setup a list to iterate...
        tsSOccupSbtPctList = []
        #Create a list of all the SOccupID values...
        tsSOccupList = df_tsSOccupSbtPct['SOccupID'].unique().tolist()
        #Populate tsSOccupSbtPctList list...
        for x in sorted(tsSOccupList):
            whereClause = "SOccupID == '" + x + "'"
            y = df_tsSOccupSbtPct.query(whereClause).to_dict('records')
            tsSOccupSbtPctList.append([x,y])
            
        # Create new field and normalize data in that field...
        df_Csv['occtype2'] = np.where(df_Csv.occtype.str.match('RES1-.*'), 'RES1', df_Csv.occtype) #normalize RES1-1NSB, etc to RES1
        df_Csv['occtype2'].replace(r'(RES3[ABCDEF])I', r'\1', regex=True, inplace=True) #drop trailing i from RES3X

        #Update tsSOccupSbtPctList; add number of rows to sample for each eqSBTID...
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
            SOccupID = OccType.rstrip()
            LenOccType = len(df_Csv[df_Csv['occtype2'] == SOccupID])
            print(SOccupID, "(", LenOccType, ")")

            if LenOccType > 0:
                # Sort by Group then Pct values, with largest at the end...
                sortedList = sorted(x[1], key = lambda i: (i['group'], i['Pct']))
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
                        if eqSBTID == 'MH  ':
                            #Create a sample using the smpCnt
                            df_Sample = df_Csv.loc[(df_Csv.occtype2 == SOccupID) & (df_Csv.eqsbtid =='NULL')].sample(n=smpCnt, replace=False, weights='mhweightareasqft')
                        elif eqSBTID == 'W1  ':
                            #Create a sample using the smpCnt
                            df_Sample = df_Csv.loc[(df_Csv.occtype2 == SOccupID) & (df_Csv.eqsbtid =='NULL')].sample(n=smpCnt, replace=False, weights='w1weightareasqft')
                        elif eqSBTID == 'URML':
                            #Create a sample using the smpCnt
                            df_Sample = df_Csv.loc[(df_Csv.occtype2 == SOccupID) & (df_Csv.eqsbtid =='NULL')].sample(n=smpCnt, replace=False, weights='urmweighteqcldesignlevel')
                            #df_Sample = df_Csv.loc[(df_Csv.occtype2 == SOccupID) & (df_Csv.eqsbtid =='NULL') & (df_Csv.eqdesignlevelid < 2)].sample(n=smpCnt, replace=False, weights='urmweighteqcldesignlevel')
                        else:
                            #Create a sample using the smpCnt
                            df_Sample = df_Csv.loc[(df_Csv.occtype2 == SOccupID) & (df_Csv.eqsbtid =='NULL')].sample(n=smpCnt, replace=False)
                        #Get a list of the samples' id's
                        SampleIDs = df_Sample.index.values.tolist()
                        #Modify those sample id's in the main dataframe
                        for ID in SampleIDs:
                            df_Csv.at[ID, 'eqsbtid'] = eqSBTID
                        print(" ", eqSBTID, len(SampleIDs))
                    
                #Assign eqSBTID from biggestPctList which is just one item as a dictionary...
                #from the dictionary...
                eqSBTID = biggestPctList['eqSBTID']
                #query on OccType and eqSBTID is null
                SOccupID = OccType.rstrip()
                #Set all remaining null values in eqSBTID to this eqSBTID...
                df_Csv.loc[(df_Csv['occtype2'] == SOccupID) & (df_Csv['eqsbtid'] =='NULL'), 'eqsbtid'] = eqSBTID
                lenMajorPercent = len(df_Csv[(df_Csv['occtype2'] == SOccupID) & (df_Csv['eqsbtid'] == eqSBTID)])
                print(" ", eqSBTID, lenMajorPercent)
                print()
            
        #eqBldgType in Hazus Model has trailing spaces to four characters (ie 'S3  ')...
        self.eqBldgTypeDisplayOrderDict = df_eqBldgTypeDisplayOrder.to_dict('records')
        df_Csv['eqbldgtypeid'] = df_Csv.apply(lambda x: self.eqbldgtypeidFunction(x['eqsbtid']), axis=1)

        #Post Process
        #Special Rule 1
        print('Pre Rule: eqbldgtypeid=1 and eqdesignlevelid = 1', len(df_Csv[(df_Csv['eqbldgtypeid'] == 1) & (df_Csv['eqdesignlevelid'] == 1)]))
        print('Pre Rule: eqbldgtypeid=1 and eqdesignlevelid = 2', len(df_Csv[(df_Csv['eqbldgtypeid'] == 1) & (df_Csv['eqdesignlevelid'] == 2)]))
        if state == 'CA':
            df_Csv.loc[(df_Csv['eqbldgtypeid'] == 1) & ((df_Csv['eqdesignlevelid'] == 1)|(df_Csv['eqdesignlevelid'] == 2)), ['eqdesignlevelid']] = 3
        else:
            df_Csv.loc[(df_Csv['eqbldgtypeid'] == 1) & (df_Csv['eqdesignlevelid'] == 1), ['eqdesignlevelid']] = 2
        print('Post Rule: eqbldgtypeid=1 and eqdesignlevelid = 1', len(df_Csv[(df_Csv['eqbldgtypeid'] == 1) & (df_Csv['eqdesignlevelid'] == 1)]))
        print('Post Rule: eqbldgtypeid=1 and eqdesignlevelid = 2', len(df_Csv[(df_Csv['eqbldgtypeid'] == 1) & (df_Csv['eqdesignlevelid'] == 2)]))
        
        '''EXPORT DATA:'''
        df_Csv.to_csv(self.NSICsvOutputPath)

    def ListGroupFunction(self, eqSBTID):
        '''Setup groups for sorting later'''
        if eqSBTID in ('MH  '):
            return 1
        elif eqSBTID in ('W1  ','C3L ','URML'):
            return 2
        else:
            return 9
        
    def SOccTypeIdFunction(self, OccType):
        '''Set the occupancy field to have match the values in sql

        Notes:
            Sample Inputs: 
                RES1-1SNB; COM9; RES3A; RES3AI; etc 
            Need to pad with spaces on the right to match sql server table:
                RES1, COM9 , RES3A
        '''
        foo = OccType.split('-')[0]
        return foo[0:5].ljust(5,' ') #add trailing whitespace

    def hzSqFtFactorsFunction(self, sqft, occupancy5):
        '''Update AreaSqFt field via lookup to CDMS hz.sqftfactors'''
        df_hzSqFtFactorsDict = self.df_hzSqFtFactors.to_dict('records')
        if sqft == 0:
            for x in df_hzSqFtFactorsDict:
                if occupancy5 == x['Occupancy']:
                    return x['SquareFootage']
        else:
            return sqft
    
    def BasementIDFunction(self, Found_Type):
        '''    [FoundTypeId] (Based on Found_Type and BasementID in Hazus_model_data.flFoundationType, watch for truncation (first 4 letters only) in NSI 2.0 data)
        Hazus_model_data.flFoundationType
        Does this need to be dynamic? Could look up values from table, then check to see if input is in any values (ie "Craw" in "Crawl Space")
        '''
        if Found_Type.lower() in ['pile']:
            #Pile
            code = 1
        elif Found_Type.lower() in ['pier']:
            #Pier
            code = 2
        elif Found_Type.lower() in ['soli', 'solidwall', 'solid wall']:
            #Solid Wall
            code = 3
        elif Found_Type.lower() in ['base', 'basement']:
            #Basement/Garden
            code = 4
        elif Found_Type.lower() in ['craw', 'crawl', 'crawl space']:
            #Crawl Space
            code = 5
        elif Found_Type.lower() in ['fill']:
            #Fill
            code = 6
        elif Found_Type.lower() in ['slab', 'slab on grade']:
            #Slab on Grade
            code = 7
        else:
            code = 0
        return code

    def YearBuiltEqDesignLevelFunction(self, state, year):
        '''    [EqDesignLevelId] Based on shapefiles Med_Yr_Blt and table 3.5, assign eqClDesignLevel.
        [Hazus_model].[dbo].[eqClDesignLevel]'''
        if state == 'RI':
            if year <= 1940:
                #code = 'PC'
                code = 1
            elif year >= 1941 and year<= 1975:
                #code =  'PC'
                code = 1
            elif year >= 1976 and year <= 1994:
                #code =  'PC'
                code = 1
            elif year >= 1995 and year <= 2000:
                #code = 'LC'
                code = 2
            elif year >= 2001:
                #code = 'LC'
                code = 2
        elif state == 'AK':
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
                code = 2
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

    def urmweighteqcldesignlevelFunction(self, eqdesignlevelid):
        if eqdesignlevelid == 1:
            return 0.90
        elif eqdesignlevelid == 2:
            return 0.09
        else:
            return 0.01

    def eqbldgtypeidFunction(self, eqBldgType):
        eqBldgType = eqBldgType.ljust(4,' ')
        for item in self.eqBldgTypeDisplayOrderDict:
            if eqBldgType == item['eqBldgType']:
                return item['DisplayOrder']
        else:
            return -9

if __name__=="__main__":
    startTime = time.time()
    print(time.ctime(startTime))
    print("startTime:", time.ctime(startTime))

    createTables = 0
    runNSI = 1
    
    if createTables == 1:
        print('Creating input tables...')
        _state = 'RI'
        testsqlTable = sqlTable(state=_state, outputDir='C:\workspace\RI_Ed\Input')
        tsSOccupSbtPct = testsqlTable.get_tsSOccupSbtPct()
        clOccupancy = testsqlTable.get_clOccupancy()
        eqBldgTypeDisplayOrder = testsqlTable.get_eqBldgTypeDisplayOrder()
        hzSqFtFactors = testsqlTable.get_hzSqFtFactors()
        print('Done.')

    if runNSI == 1:
        print('Run NSI Updater...')
        _NSI = NSI(state='RI', 
                    inputDir='C:\workspace\RI_Ed\Input', 
                    outputDir='C:\workspace\RI_Ed\Output', 
                    NSIFileName='RI_NSI.txt',
                    OutFileName='RI_NSI_20211109_1A.csv')
        print('Done.')

    endTime = time.time()
    print("endTime:", time.ctime(endTime))
    print("Elapsed Time (Hour:Minute:Seconds):", str(timedelta(seconds=endTime-startTime)))