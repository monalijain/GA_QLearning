__author__ = 'Monali'

import time
from datetime import timedelta, datetime
from DBUtils import *
import logging

from NonSortedGANewVersion import *
import GlobalVariables as gv
from DB_MakePerf import Make_Performance_Measures
from FastNonDominatedSort import FastNonDominatedSort

def processWalkforward(startDate,endDate,dbObject,MaxIndividuals,MaxGen,number):
    print startDate
    print endDate
    string_1= "CREATE TABLE performance_measures_"+"Training"+"_walk" + str(number)+ "_" + "stock"+ str(gv.stock_number)
    logging.info("Executing Query %s",string_1)
    dbObject.dbQuery(" " + string_1 + " "
                             "("
                             "individual_id int,"
                             "netpl_trades float,"
                             "netpl_drawdown float,"
                             "total_drawup float,"
                             "total_drawdown float,"
                             "netpl float,"
                             "total_trades int,"
                             "profit_epochs float"
                             ")")

    logging.info("Calculating Performance Measures of Training Period for walkforward %s",str(startDate))
    Make_Performance_Measures(startDate,endDate,Train_Or_Report="Training",walkforward_number=number)


    logging.info("Calling NonSorted Genetic Algorithm for walkforward %s",1)
    [A,C,StoreParetoID]=NonSortedGA(1,gv.MaxIndividualsInGen,MaxGen,MaxIndividuals,dbObject)

    F=FastNonDominatedSort(A)
    numFronts=max(F.keys())
    rank=1
    for front in range(1,numFronts+1):
		for individual in F[front].keys():
			#print (individual,rank)
			dbObject.updateRank(individual, rank)
			rank=rank+1
		
	
	