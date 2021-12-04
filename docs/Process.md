# PREPROCESS
#### Table 1: Input Tables
| Table Name                | Fields |
|---------------------------|--------|
|StateDB.tsSOccupSbtPct     | SchemeID,SOccupID,eqSBTID,Pct |
| Hazus_model.clOccupancy   | Occupancy,SoccID |
| Hazus_model.eqclBldgType  | eqBldgType,DisplayOrder |
| CDMS.hzSqftFactors        | Occupancy,SquareFootage |
| NSI data                  | See the next table |
There are also some hardcoded values in python functions. Should move these to config files.

#### Table 2: NSI Input table
|Field Name|Data Type|Value Range|
|----------|---------|-----------|
|NsiId|Unique Id||
|found_type|text|Basement, Crawl, Fill, etc|
NSI Data as Input Table

#### Table 3: Output table
|Field Name     |Data Type  |Value Range|Notes|
|---------------|-----------|-----------|-----|
|NsiID          |Nvarchar 24|Unique Id||
|EqBldgTypeId   |smallint|||
|EqDesignLevelId|smallint|||
|SOccTypeId     |smallint|||
|FoundTypeId    |smallint|1,2,3,4,5,6,7||
|CBFips         |Nvarchar 15|i.e. 530090020001002||
|NStories       |int|||
|AreaSqft       |Numeric 38, 8||
|PerSqftAvgVal  |Numerice38, 20||
|FirstFloorHt   |Numeric 38, 8||
|ValStruct      |Numeric 38, 8||
|ValCont        |Numeric 38, 8||
|ValOther       |Numeric 38, 8||
|ValVehic       |Numeric 38, 8||
|MedYrBlt       |Int|1995, 0 okay for EDU||
|Pop2pmU65      |Int|||
|Pop2pmO65      |Int|||
|Pop2amU65      |Int|||
|Pop2amO65      |Int|||
|Latitude       |Numeric 38, 8||Need to calculate|
|Longitude      |Numeric 38, 8||Need to calculate|
The output from the script should match this table  (tsNsiGbs table in statedb)

