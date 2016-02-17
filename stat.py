#!/usr/bin/env python
#-*- coding:utf-8 -*-
import threading
import time
import subprocess
import Global

#дlog����
#def appQueryLog(line):

class Monitor:
    def __init__(self):
        print "Monitor init!"

    def getstat(self):
        while True:
            time.sleep(10)
            statlog = time.strftime("%Y-%m-%d %H:%M:%S\t", time.localtime()) 
            

    def start(self):

        self.t_stat = threading.Thread(target = getstat)#����ͳ���߳�
        self.t_stat.start()



#��־�ļ�һ���ǰ����������ͨ���ڳ������ж��ļ��Ĳ��������뵱ǰʱ�䣬������ص���־�ļ�
def monitorLog(logFile):
    t_stat = threading.Thread(target = getstat)
    t_stat.start()
    print '��ص���־�ļ���: %s' % logFile
    popen = subprocess.Popen('tail -f ' + logFile, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    pid = popen.pid
    print('Popen.pid:' + str(pid))
    while True:
        line = popen.stdout.readline().strip()
        # �ж������Ƿ�Ϊ��
        if line:
            #print(line)
            row = Row(line)
            row.dealdata()
            
        
        # ��ǰʱ��
        thistime = 'log/%d_%d_%d.query.log' % (time.localtime().tm_year , time.localtime().tm_mon , time.localtime().tm_mday)
        if thistime != logFile:
            # ��ֹ�ӽ���
            popen.kill()
            print 'ɱ��subprocess'
            break
    #���ܴ��ڵ����������������
    monitorLog(thistime)


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

def getstat():
    while True:
        time.sleep(10)

        statlog = time.strftime("%Y-%m-%d %H:%M:%S\t", time.localtime()) 
#        statlog = '%d_%d_%d %d:%d:%d\t' % (time.localtime().tm_year , time.localtime().tm_mon , time.localtime().tm_mday, time.localtime().tm_hour, time.localtime().tm_min, time.localtime().tm_sec)  
        for type in Global.type_dict.keys():
            if type.find("_last") == -1:
                type_last = type + "_last"
                inc = Global.type_dict[type] - Global.type_dict[type_last]
                Global.type_dict[type_last] =  Global.type_dict[type] 
                statlog += '%s:%d\t' % (type,inc)

        statlog += '\n'
        print statlog





        




#������
if __name__ == '__main__':
    logFile = 'log/%d_%d_%d.query.log' % (time.localtime().tm_year , time.localtime().tm_mon , time.localtime().tm_mday)  
    monitorLog(logFile)
