__author__ = 'MJ'

from Wrapper import processWalkforward
from datetime import datetime,timedelta
import calendar
class Ranking:
    def updateRankings(self, startDate, endDate, dbObject,MaxIndividuals,MaxGen,number):
        processWalkforward(startDate.isoformat(),endDate.isoformat(),dbObject,MaxIndividuals,MaxGen,number) #ranks the pareto optimal front
        
'''
if __name__ == "__main__":
    rankingObject = Ranking()
    dbObject = DBUtils()
    dbObject.dbConnect()
    date = datetime(2012, 4, 2).date()
    #print date
    periodEndDate = datetime(2012, 4, 10).date()
    #print( periodEndDate)
    rankingObject.updateRankings(date, periodEndDate, dbObject)
    dbObject.dbClose()
'''