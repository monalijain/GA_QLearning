__author__ = 'Ciddhi'

from datetime import timedelta, datetime

databaseName = 'mkt_sbi'                            # This is database name to which connection is made
userName = 'root'                               # This is the user name for database connection
password = 'controljp'                          # This is the password for database connection
db_host = '127.0.0.1'
db_port = '3306'
startDate = datetime(2012, 1, 1).date()         # This is the start of trading period
endDate = datetime(2012, 3, 31).date()           # This is the end of trading period
alpha = 0.8                         # This defines the weightage to long trades as compared to short trades while constructing reward matrix
gamma = 0.9                         # This defines the weightage of old data as compared to latest observations of reward matrix
maxGreedyLevel = 3
dummyIndividualId = -1               # This is to keep a track of max total capital that is invested in the portfolio
unitQty = 250000                    # This is the amount of each decrement in asset
hourWindow = 1                      # This is the window after which re-allocation is done
maxTotalAsset = 10000000            # This is the total asset deployed
trainingFactor = 2
trainingMaxTotalAsset = maxTotalAsset*trainingFactor        # This is the total asset deployed while training
factor = 2
maxAsset = maxTotalAsset/factor     # This is the maximum asset an individual can use
zeroRange = 0.001                   # This determines the spread between states 0, 1, 2
aggregationUnit = 1
maxThreads = 8                     # This is the4 maximum number of threads that can run concurrently


stock_number=1 #Stock number
cost_of_trading=0.0 #Cost of trading
MaxIndividualsInGen=2500 #Maximum Individuals that you want in each generation
priceSeriesTable="price_series_table" #Name of the price series table in the database


#Dont Change the variables given below.

#numDaysInTraining=20 #Number of days in training period
#numDaysInReporting=10 # Number of days in testing period

name_database=databaseName # Name of the database where, PriceSeries.csv and name_Tradesheet_table is stored
db_username = userName
db_password = password

#The columns in tradsheet table are:
#TradeID, IndividualID, TradeType, EntryDate, EntryTime, EntryPrice, EntryQty, ExitDate, ExitTime, ExitPrice
name_Tradesheet_Table="old_tradesheet_data_table" #name of the tradesheet table (fixed)


#Dont change these for now
MinimumGen=2 #Minimum Generations for which the program should run, provided those many individuals exist
CheckGen=3 #Number of generations for which the convergence will be checked
ConvergenceValue=0.01 #Value of NetPL/Total Trades

