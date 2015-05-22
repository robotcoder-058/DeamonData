__author__ = 'chenyi'
import Reporter
import os
import socket
import datetime
import pickle


class Handler():
    def __init__(self, path, num):
        self.socket_path = path
        self.socket_num = num
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        print 'hava socket'

    def startListen(self):
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        self.socket.bind(self.socket_path)
        self.socket.listen(self.socket_num)
        print("start listen")

    def start(self):
        self.startListen()
        self.receiveloop()


    def paser_and_result(self, data):
        data_real = pickle.loads(data)

        def selectaction():
            startDate = datetime.datetime.fromtimestamp(int(data_real['data']['startDate']) / 1000)
            endDate = datetime.datetime.fromtimestamp(int(data_real['data']['endDate']) / 1000 )

            temp = data_real['data']['startTime'].split(":")
            startTime = {'H': int(temp[0]), 'M': int(temp[1])}

            temp = data_real['data']['endTime'].split(":")
            endTime = {'H': int(temp[0]), 'M': int(temp[1])}

            timelist = []
            for i in range((endDate - startDate).days):
                start = startDate + datetime.timedelta(days=i) + \
                        datetime.timedelta(hours=startTime['H'], minutes=startTime['M'])
                end = endDate + datetime.timedelta(days=i) + \
                        datetime.timedelta(hours=endTime['H'], minutes=endTime['M'])
                timelist.append([start, end])

            objects = []
            for i in data_real['data']['selectedObjects']:
                objects.append(int(i))

            reporter = Reporter.selectDateReporter(timelist, objects, data_real['data']['selectOptions'])

            return reporter


        def lastdayaction():
            reporter = Reporter.LastDayReporter()
            return reporter

        def lastweekaction():
            reporter = Reporter.LastWeekReporter()
            return reporter

        def lastmonthaction():
            reporter = Reporter.LastMonthReporter()
            return reporter

        def lastyearaction():
            reporter = Reporter.LastYearReporter()
            return reporter


        orderflag = {
            "d": lastdayaction,
            'm': lastmonthaction,
            'w': lastweekaction,
            'y': lastyearaction,
            's': selectaction
        }
        result = orderflag.get(data_real["flag"])()
        return result.assemble()

    def receiveloop(self):
        while True:
            cli_socket, addr = self.socket.accept()

            while True:
                recv_data = cli_socket.recv(1024)
                if not recv_data:
                    break
                print 'received from client:', recv_data
                cli_socket.send(self.paser_and_result(recv_data))

            cli_socket.close()

    def close(self):
        self.socket.close()




























