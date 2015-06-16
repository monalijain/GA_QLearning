__author__ = 'Ciddhi'

from DBUtils import *
from datetime import datetime,timedelta
import calendar
class PerformanceMeasures:

    def CalculateTradesheetPerformanceMeasures(self, startDate, endDate, dbObject):
        resultLongNetPL = dbObject.getLongNetPL(startDate, endDate)
        resultShortNetPL = dbObject.getShortNetPL(startDate, endDate)
        resultLongTrades = dbObject.getLongTrades(startDate, endDate)
        resultShortTrades = dbObject.getShortTrades(startDate, endDate)
        netPL = 0
        totalTrades = 0
        for pl, dummy in resultLongNetPL:
            if pl:
                netPL = netPL + pl
        for pl, dummy in resultShortNetPL:
            if pl:
                netPL = netPL + pl
        for trades, dummy in resultLongTrades:
            if trades:
                totalTrades = totalTrades + trades
        for trades, dummy in resultShortTrades:
            if trades:
                totalTrades = totalTrades + trades
        performance = netPL/totalTrades
        print('Total trades taken : ')
        print(totalTrades)
        print('Performance : ')
        print(performance)
        return performance

    def CalculateReferenceTradesheetPerformanceMeasures(self, startDate, endDate, dbObject):
        resultLongNetPL = dbObject.getRefLongNetPL(startDate, endDate)
        resultShortNetPL = dbObject.getRefShortNetPL(startDate, endDate)
        resultLongTrades = dbObject.getRefLongTrades(startDate, endDate)
        resultShortTrades = dbObject.getRefShortTrades(startDate, endDate)
        netPL = 0
        totalTrades = 0
        for pl, dummy in resultLongNetPL:
            netPL = netPL + pl
        for pl, dummy in resultShortNetPL:
            netPL = netPL + pl
        for trades, dummy in resultLongTrades:
            totalTrades = totalTrades + trades
        for trades, dummy in resultShortTrades:
            totalTrades = totalTrades + trades
        performace = netPL/totalTrades
        print(totalTrades)
        print(performace)
        return performace


if __name__ == "__main__":
    dbObject = DBUtils()
    performanceObject = PerformanceMeasures()
    dbObject=DBUtils()
    dbObject.dbConnect()

    mtm=[]
    date = datetime(2012, 3, 1).date()
    periodEndDate=datetime(2012,3,31).date()
    performanceObject.CalculateTradesheetPerformanceMeasures(date, periodEndDate, dbObject)
    #performanceObject.CalculateReferenceTradesheetPerformanceMeasures(date, periodEndDate, dbObject)
    walkforwardStartDate = date
    #date = datetime.datetime(2003,8,1,12,4,5)
    for i in range(0,31):
        #print(walkforwardStartDate.isoformat())
        result=dbObject.dbQuery("SELECT sum(mtm), 1 from mtm_table WHERE date = '"+str(walkforwardStartDate)+"'")
        for sum, dummy in result:
            if(sum==None):

                mtm.append(0)
            else:
                mtm.append(sum)


        walkforwardStartDate = walkforwardStartDate + timedelta(days=1)

    #print mtm
    date_count_range=31
    num_days_DD=[]
    DD_History=[]
    if(mtm[0] < 0):
        DD_History.append(mtm[0])
        num_days_DD.append(1)
    else:
        DD_History.append(0)
    DD_date_count = 1
    while(DD_date_count < date_count_range):
        if(mtm[DD_date_count]<0):
            DD_History.append(DD_History[DD_date_count-1]+mtm[DD_date_count])
        else:
            DD_History.append(0)
        DD_date_count = DD_date_count +1

    print(DD_History)
    total_DD=0
    for DD_Daily_Value in DD_History:
        if(DD_Daily_Value<total_DD):
            total_DD=DD_Daily_Value

    #print total_DD, "total_DD", "2 days"


    Gain_History=[]
    if(mtm[0]>0):
        Gain_History.append(mtm[0])
    else:
        Gain_History.append(0)
    Gain_date_count=1
    while(Gain_date_count <date_count_range):
        if(mtm[Gain_date_count]>0):
            Gain_History.append(Gain_History[Gain_date_count-1]+mtm[Gain_date_count])
        else:
            Gain_History.append(0)
        Gain_date_count+=1

    total_Gain=0.0
    for Gain_Daily_Value in Gain_History:
        if(Gain_Daily_Value>total_Gain):
            total_Gain=Gain_Daily_Value

    #print total_Gain,"total_gain", "2 days"

    print Gain_History

    dbObject.dbClose()

