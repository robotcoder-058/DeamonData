__author__ = 'chenyi'

import  pandas

DEFAULTACCOUNTLIST = [1000002,1000001,1000003,1000011,
                      1000012,1000017,1000018,1000030,1000031,
                      1000032,1000033,1000034,1000037,1000049,
                      1000061,1000093,1000048]


DATAPATH = "/Users/chenyi/Desktop/readyfile.csv"

class Processer():

    def __init__(self ):
        self.__mainData = pandas.read_csv(DATAPATH)
        self.__mainData = self.__mainData.drop('Unnamed: 0',axis = 1)
        self.__mainData.JNDATETIME = pandas.to_datetime(self.__mainData.JNDATETIME)

        self.__mainData = self.__mainData.set_index("JNDATETIME")
        self.__mainData = self.__mainData.sort_index()
        self.__mainData = self.__mainData[self.__mainData.TRANAMT < 0]
        self.__mainData.TRANAMT = abs(self.__mainData.TRANAMT)
        print self.__mainData.info()

        self.__startDateTime = None
        self.__endDateTime = None
        self.__objectes = None
        self.__tmpData = None

    def setDateTime(self, startDateTiem , endDateTime):
        self.__startDateTime = startDateTiem
        self.__endDateTime = endDateTime

    def setObjects(self, objectes = DEFAULTACCOUNTLIST):
        self.__objectes = objectes
        print self.__objectes
        print type(self.__objectes)

    def ready(self):
        self.__tmpData = self.__mainData[self.__mainData["TOACCOUNT"].isin(self.__objectes)]
        self.__tmpData = self.__tmpData[self.__startDateTime : self.__endDateTime]

    def readyforselect(self, timelist):
        self.__tmpData = self.__mainData[self.__mainData["TOACCOUNT"].isin(self.__objectes)]
        length = len(timelist)
        tmp = self.__tmpData[timelist[0][0] : timelist[0][1]]
        for i in range(1,length):
            tmp.append(self.__tmpData[timelist[i][0] : timelist[i][1]])

        print(tmp.info())

        self.__tmpData = tmp

    def getMean(self):
        return self.__tmpData.TRANAMT.mean()

    def getSum(self):
        return self.__tmpData.TRANAMT.sum()

    def getTimeSum(self):
        return self.__tmpData.TRANAMT.count()

    def getAccountGroupByTime(self):
        return self.__tmpData.groupby("TOACCOUNT").TRANAMT.count()

    def getAccountGroupBySum(self):
        return self.__tmpData.groupby("TOACCOUNT").TRANAMT.sum()

    def getAccountGroupByCount(self):
        return self.__tmpData.groupby("TOACCOUNT").TRANAMT.count()

    def getAccountGroupByMean(self):
        return self.__tmpData.groupby("TOACCOUNT").TRANAMT.mean()

    def getDaySum(self):
        return  self.__tmpData.groupby("FROMACCOUNT").TRANAMT.resample("D", how = "sum").mean()

    def getResampleResult(self, freq, method):
        return self.__tmpData.TRANAMT.resample(freq , method)

    def getEveryResample(self, freq, method):
        return self.__tmpData.groupby("TOACCOUNT").TRANAMT.resample(freq, how = method)

    def clear(self):
        self.__startDateTime = None
        self.__endDateTime = None
        self.__objectes = None
        self.__tmpData = None


processer = Processer()

if processer:
    print 'finish load data'

def getProcesser():
    return processer
