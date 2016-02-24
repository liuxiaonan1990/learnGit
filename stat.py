#!/usr/bin/env python
#-*- coding:utf-8 -*-
import threading
import time
import subprocess
import Global
import signal
import logging 

#写log方法
#def appQueryLog(line):
class Logger:        
    def __init__(self, logName, logFile):
        self._logger = logging.getLogger(logName)
        handler = logging.FileHandler(logFile)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
        self._logger.setLevel(logging.INFO)

    def log(self, msg):
        if self._logger is not None:
            self._logger.info(msg)





class Monitor:
    def __init__(self):
        self.pid = 30000
        print "Monitor init!"

    def initlog(self):
        self.debuglogger = Logger("debuglogger",'statlog/%d_%d_%d.debug.log' % (time.localtime().tm_year , time.localtime().tm_mon , time.localtime().tm_mday))
        self.debuglogger.log("debuglog init")
        self.statlogger = Logger("statlogger",'statlog/%d_%d_%d.stat.log' % (time.localtime().tm_year , time.localtime().tm_mon , time.localtime().tm_mday))
        self.debuglogger.log("statlog init")
        self.querylogger = Logger("querylogger",'statlog/%d_%d_%d.query.log' % (time.localtime().tm_year , time.localtime().tm_mon , time.localtime().tm_mday))
        self.debuglogger.log("querylog init")
        self.errorlogger = Logger("errorlogger",'statlog/%d_%d_%d.error.log' % (time.localtime().tm_year , time.localtime().tm_mon , time.localtime().tm_mday))
        self.debuglogger.log("errorlog init")



    def getstat(self):
        while True:
            time.sleep(10)

#            statlog = time.strftime("%Y-%m-%d %H:%M:%S\t", time.localtime()) 
            statlog = ""
            for type in Global.type_dict.keys():
                if type.find("_last") == -1:
                    type_last = type + "_last"
                    inc = Global.type_dict[type] - Global.type_dict[type_last]
                    Global.type_dict[type_last] =  Global.type_dict[type] 
                    statlog += '%s:%d\t' % (type,inc)

            statlog += '\n'
            print statlog
            self.statlogger.log(statlog)


    #日志文件一般是按天产生，则通过在程序中判断文件的产生日期与当前时间，更换监控的日志文件
    def run(self):
        print '监控的日志文件是: %s' % self.logFile
        self.debuglogger.log('监控的日志文件是: %s' % self.logFile)
        self.popen = subprocess.Popen('tail -f ' + self.logFile, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        self.pid = self.popen.pid
        print('Popen.pid:' + str(self.pid))
        self.debuglogger.log('Popen.pid:' + str(self.pid))
        while True:
            line = self.popen.stdout.readline().strip()
            # 判断内容是否为空
            if line:
                #print(line)
                row = Row(line)
                row.dealdata()
                self.querylogger.log(row.log)
                
            
            # 当前时间
            thistime = 'log/%d_%d_%d.query.log' % (time.localtime().tm_year , time.localtime().tm_mon , time.localtime().tm_mday)
            self.errorlogger.log('%s, %s' % (thistime, self.logFile))
            if thistime != self.logFile:
                # 终止子进程
                self.popen.kill()
                print '杀死subprocess'
                self.logFile = thistime
                self.debuglogger.log('杀死subprocess')
                break
        #可能存在迭代层数过深的问题
        self.run()


    def start(self):

        self.initlog()
        self.t_stat = threading.Thread(target = Monitor.getstat, args=(self,))#启动统计线程
        self.t_stat.setDaemon(True)
        self.t_stat.start()

        self.logFile = 'log/%d_%d_%d.query.log' % (time.localtime().tm_year , time.localtime().tm_mon , time.localtime().tm_mday)  #当前日志
        self.run()

    def __del__(self):
        print "del Monitor"
        



class Row:
    def __init__(self, data):
        #data = "2016-01-29 00:00:02     user=867148029720587&prod=KuwoMusic_ar_kwplayer_ar_7.5.2.0&source=kwplayer_ar_7.5.2.0_18test.apk&sourcetype=1&type=tips&word=%E8%80%81%E4%BA%BA&encoding=utf-8&br=128kaac&ktype=1"
        if data.find('\t') != -1:
            self.data = data.split('\t')[1]
        else:
            self.data = data
        self.type = "NULL" 
        self.sourcetype = "0"
        self.prod = "NULL"
        self.source = "NULL"
        self.all = "NULL"
        self.rid = "NULL"
        self.id = "NULL"
        self.word = "NULL"
        self.log = "NULL"

    #data = "user=867148029720587&prod=KuwoMusic_ar_kwplayer_ar_7.5.2.0&source=kwplayer_ar_7.5.2.0_18test.apk&sourcetype=1&type=tips&word=%E8%80%81%E4%BA%BA&encoding=utf-8&br=128kaac&ktype=1"

    def dealdata(self):

        Global.total += 1
        Global.type_dict["total"] += 1
        Global.sourcetype_dict["total"] += 1
        Global.source_dict["total"] += 1
        Global.prod_dict["total"] += 1

        list = self.data.split("&")

        #print self.data
        for node in list:
            if node.find("=") != -1:
                key = node.split("=")[0]    
                value = node.split("=")[1]
            else:
                continue

            if key == "type":
                self.type = value
                Global.type_dict.setdefault(value,0)
                if(value.find("_last") == -1):
                    Global.type_dict.setdefault(value + "_last",0)
                Global.type_dict[value] += 1
            elif key == "source":
                self.source = value
                Global.source_dict.setdefault(value,0)
                Global.source_dict[value] += 1
            elif key == "prod":
                self.prod = value
                Global.prod_dict.setdefault(value,0)
                Global.prod_dict[value] += 1
            elif key == "sourcetype":
                self.sourcetype = value
                Global.sourcetype_dict.setdefault(value,0)
                Global.sourcetype_dict[value] += 1
            elif key == "id":
                self.id = value
            elif key == "rid":
                self.rid = value
            elif key == "all":
                self.all = value
            elif key == "word":
                self.word = value

        self.log = "type=%s&source=%s&sourcetype=%sprod=%s&all=%s&rid=%s&id=%s&word=%s" % (self.type, self.source, self.sourcetype, self.prod, self.all, self.rid, self.id, self.word)

        #print self.log 
        #print Global.type_dict


        





#主函数
if __name__ == '__main__':
    try:
        monitor = Monitor()
        monitor.start()
    except KeyboardInterrupt:
        print "kill monitor"
        exit()
