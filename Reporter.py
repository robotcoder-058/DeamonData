#-*- coding: UTF-8 -*-
__author__ = 'chenyi'

import Processer
import datetime
import json
import pandas


ACCOUNTNAME = {'1000002':'一期二食堂',
               '1000003':'一期三食堂',
               '1000030':'东区四食堂',
               '1000001':'一期一食堂',
               '1000011':'二期五食堂',
               '1000012':'二期六食堂',
               '1000017':'二期七食堂',
               '1000018':'二期八食堂',
               '1000031':'东区五食堂',
               '1000032':'东区七食堂',
               '1000033':'东区八食堂',
               '1000034':'本部方塔',
               '1000037':'北区第六',
               '1000049':'东区塔影',
               '1000061':'独墅湖留学生',
               '1000093':'阳澄湖三食堂',
               '1000048':'本部教工'}


class Reporter():
    def __init__(self):
        self.processser = Processer.getProcesser()

    def assemble(self):
        pass

    def trainMean(self):
        return self.processser.getMean()

    def trainSum(self):
        return self.processser.getSum()

    def trainTimeSum(self):
        return self.processser.getTimeSum()

    def GroupByTime(self):
        return self.processser.getAccountGroupByCount()

    def GroupBySum(self):
        return self.processser.getAccountGroupBySum()

    def GroupByMean(self):
        return self.processser.getAccountGroupByMean()

    def DaySum(self):
        return self.processser.getDaySum()



class LastDayReporter(Reporter):
    def __init__(self):
        Reporter.__init__(self)
        now = datetime.datetime.now()
        self.processser.clear()
        self.__startDateTime = datetime.datetime(now.year, now.month, now.day - 1)
        self.__endDateTime = datetime.datetime(now.year, now.month, now.day + 1)
        self.processser.setDateTime(self.__startDateTime, self.__endDateTime)
        self.processser.setObjects()
        self.processser.ready()

    def dayResample(self):
        return self.processser.getResampleResult("10min", sum)

    def assemble(self):
        result = {}
        result["sum"] = int(self.trainSum())
        result["timesum"] = int(self.trainTimeSum())
        result["trainmean"] = int(self.trainMean())
        result["groupbysum"] = dict(self.GroupBySum())
        result["groupbyTime"] = dict(self.GroupByTime())
        # result["daysum"] = int(self.DaySum())
        result["resample"] = dict(self.dayResample())


        return cover_to_json(result)

class LastWeekReporter(Reporter):
    def __init__(self):
        Reporter.__init__(self)
        now = datetime.datetime.now()
        self.processser.clear()
        self.__startDateTime = now - datetime.timedelta(days=(now.weekday() + 7), hours=now.hour, minutes=now.minute)
        self.__endDateTime = now - datetime.timedelta(days=now.weekday(), hours=now.hour, minutes=now.minute)
        self.processser.setDateTime(self.__startDateTime, self.__endDateTime)
        self.processser.setObjects()
        self.processser.ready()

    def dayResample(self):
        return self.processser.getResampleResult("2H", sum)

    def assemble(self):
        result = {}
        result["sum"] = int(self.trainSum())
        result["timesum"] = int(self.trainTimeSum())
        result["trainmean"] = int(self.trainMean())
        result["groupbysum"] = dict(self.GroupBySum())
        result["groupbyTime"] = dict(self.GroupByTime())
        # result["daysum"] = int(self.DaySum())
        result["resample"] = dict(self.dayResample())


        return cover_to_json(result)

class LastMonthReporter(Reporter):
    def __init__(self):
        Reporter.__init__(self)
        now = datetime.datetime.now()
        self.processser.clear()
        self.__startDateTime = datetime.datetime(now.year, now.month - 1,1)
        self.__endDateTime = datetime.datetime(now.year, now.month, 1)
        self.processser.setDateTime(self.__startDateTime, self.__endDateTime)
        self.processser.setObjects()
        self.processser.ready()


    def monthResample(self):
        return self.processser.getResampleResult("D", "sum")

    def assemble(self):
        result = {}
        result["sum"] = int(self.trainSum())
        result["timesum"] = int(self.trainTimeSum())
        result["trainmean"] = int(self.trainMean())
        result["groupbysum"] = dict(self.GroupBySum())
        result["groupbyTime"] = dict(self.GroupByTime())
        # result["daysum"] = int(self.DaySum())
        result["resample"] = dict(self.monthResample())

        return cover_to_json(result)

class LastYearReporter(Reporter):
    def __init__(self):
        Reporter.__init__(self)
        now = datetime.datetime.now()
        self.processser.clear()
        self.__startDateTime = datetime.datetime(now.year - 1, 1, 1)
        self.__endDateTime = datetime.datetime(now.year, 1, 1)
        print(self.__startDateTime)
        self.processser.setDateTime(self.__startDateTime, self.__endDateTime)
        self.processser.setObjects()
        self.processser.ready()

    def yearResample(self):
        return self.processser.getResampleResult("7D", "sum")

    def assemble(self):
        result = {}
        result["sum"] = int(self.trainSum())
        result["timesum"] = int(self.trainTimeSum())
        result["trainmean"] = int(self.trainMean())
        result["groupbysum"] = dict(self.GroupBySum())
        result["groupbyTime"] = dict(self.GroupByTime())
        # result["daysum"] = int(self.DaySum())
        result["resample"] = dict(self.yearResample())

        return cover_to_json(result)

class selectDateReporter(Reporter):
    def __init__(self, timelist, objectes, options, freq='D'):
        Reporter.__init__(self)
        self.__timelist = timelist
        self.__objectes = objectes
        self.processser.clear()
        self.processser.setObjects(self.__objectes)
        self.processser.readyforselect(self.__timelist)
        self.__options = options
        self.__freq = freq

    def allchangeResample(self, method, freq='D'):
        return self.processser.getResampleResult(freq, method)

    def changeResample(self, method, freq='D'):
        return self.processser.getEveryResample(freq, method)


    def assemble(self):
        result = {}

        def resample_format(data):
            templist = []
            for i in data.keys():
                temp = []
                temp.append(str(i.value / 1000000))
                temp.append(float(data[i]))
                if pandas.isnull(temp[1]):
                    temp[1] = 0
                templist.append(temp)

            tlist = sorted(templist, key=lambda x: x[0])
            return tlist

        def all_resample_format(data):
            resultList = {};
            for i in self.__objectes:
                tmp = dict(data[i])
                templist = []
                for j in tmp.keys():
                    temp = []
                    temp.append(str(j.value / 1000000))
                    temp.append(float(tmp[j]))
                    if pandas.isnull(temp[1]):
                        temp[1] = 0
                    templist.append(temp)

                tlist = sorted(templist, key=lambda x: x[0])
                resultList[ACCOUNTNAME[str(i)]] = tlist;
            return resultList

        def group_formar(data):
            templist = []
            for i in data.keys():
                temp = {}
            try:
                temp[ACCOUNTNAME[str(i)]] = int(data["groupbysum"][i])
            except:
                temp[str(i)] = int(data["groupbysum"][i])
            templist.append(temp)

            tlist = sorted(templist, key=lambda x: x.values()[0], reverse=True)
            return tlist

        if 'sum' in self.__options:
            result['sum'] = int(self.trainSum())

        if 'timessum' in self.__options:
            result['timessum'] = int(self.trainTimeSum())

        if 'trainmean' in self.__options:
            result['trainmean'] = int(self.trainMean())

        if 'allsumchange' in self.__options:
            temp = dict(self.allchangeResample('sum', self.__freq))
            result['allsumchange'] = resample_format(temp)

        if 'alltimeschange' in self.__options:
            temp = dict(self.allchangeResample('count', self.__freq))
            print(temp)
            result['alltimeschange'] = resample_format(temp)

        if 'allmeanchange' in self.__options:
            temp = dict(self.allchangeResample('mean', self.__freq))
            result['allmeanchange'] = resample_format(temp)

        if 'sumchange' in self.__options:
            temp = self.changeResample('sum', self.__freq)
            result['sumchange'] = all_resample_format(temp)

        if 'timeschange' in self.__options:
            temp = self.changeResample('count', self.__freq)
            result['timeschange'] = all_resample_format(temp)

        if 'meanchange' in self.__options:
            temp = self.changeResample('mean', self.__freq)
            result['meanchange'] = all_resample_format(temp)

        if 'groupbysum' in self.__options:
            temp = dict(self.GroupBySum())
            result['groupbysum'] = group_formar(temp)

        if 'groupbytimes' in self.__options:
            temp = dict(self.GroupByTime())
            result['groupbytimes'] = group_formar(temp)

        if 'groupbymean' in self.__options:
            temp = dict(self.GroupByMean())
            result['groupbymean'] = group_formar(temp)

        return json.dumps(result)



def cover_to_json(data):
    sendData = dict()

    templist = []
    for i in data["groupbysum"].keys():
        temp = {}
        try:
            temp[ACCOUNTNAME[str(i)]] = int(data["groupbysum"][i])
        except:
            temp[str(i)] = int(data["groupbysum"][i])
        templist.append(temp)

    tlist = sorted(templist, key=lambda x : x.values()[0], reverse=True)

    sendData["groupbysum"] = tlist

    templist = []
    for i in data["groupbyTime"].keys():
        temp = {}
        try:
            temp[ACCOUNTNAME[str(i)]] = int(data["groupbyTime"][i])
        except:
            temp[str(i)] = int(data["groupbyTime"][i])
        templist.append(temp)

    tlist = sorted(templist, key=lambda x : x.values()[0], reverse=True)
    sendData["groupbytimes"] = tlist

    templist = []
    # timetemp = []
    # for i in data["Resample"].keys():
    #     timetemp.append(i.value)
    # timetemp.sort()
    # for i in timetemp:
    #     temp = []
    #     temp.append(str(i.value / 1000000))
    #     temp.append(float(["Resample"][i]))
    #     templist.append(temp)



    for i in data["resample"].keys():
        temp = []
        temp.append(str(i.value / 1000000))
        temp.append(float(data["resample"][i]))
        if pandas.isnull(temp[1]):
            temp[1] = 0
        templist.append(temp)

    tlist = sorted(templist, key=lambda x: x[0])
    sendData["resample"] = tlist

    sendData["sum"] = int(data["sum"])
    sendData["timesum"] = int(data["timesum"])
    sendData["trainmean"] = int(data["trainmean"])

    print sendData

    return  json.dumps(sendData)