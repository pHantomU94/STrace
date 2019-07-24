import time
import pandas as pd
import os
import json
import random
from sys import maxsize

def get_last_line(filename):
    '''
    To seek the second line from the tail of the log file
    '''
    try:
        filesize = os.path.getsize(filename)
        if filesize == 0:
            return None
        else:
            with open(filename, 'rb') as fp: # to use seek from end, must use mode 'rb'
                offset = -8                 # initialize offset
                while -offset < filesize:   # offset cannot exceed file size
                    fp.seek(offset, 2)      # read # offset chars from eof(represent by number '2')
                    lines = fp.readlines()  # read from fp to eof
                    if len(lines) >= 3:     # if contains at least 2 lines
                        return lines[-2]    # then last line is totally included
                    else:
                        offset *= 2         # enlarge offset
                fp.seek(0)
                lines = fp.readlines()
                return lines[-2]
    except FileNotFoundError:
        print(filename + ' not found!')

def timestamp(timestring):
    '''
    transfer the time form date style to timestamp, unit ms
    '''
    date_time = timestring.split(',')[0]
    ms_time = timestring.split(',')[1]
    structedtime = time.strptime(date_time, "%y/%m/%d/%H:%M:%S")
    
    time_stamp = int(time.mktime(structedtime) * 1000 + int(ms_time))
    # time to timestamp (unit ms)
    return time_stamp

class Flow(object):
    def __init__(self):
        self.src = -1       #src node
        self.dst = -1       #dst node
        self.sp = -1        #src port 
        self.dp = -1        #dst port
        self.size = -1      #flow size
        self.rtime = maxsize     #the relative time of first flow 
        self.tag = ''       #flow tag (for identification)
        self.sid = -1       #stage id

        

class Transfer(object):
    def __init__(self, dirpath, appID):
        self.count = 0      #the shuffle stage count that already processed
        self.sdict = {}     #Shuffle_id and the corresponding relationship between shuffle stage
        self.hosts = []
        self.dirpath = dirpath
        self.appID = appID
        self.host_ip = {'10.134.147.135':'net1', '10.134.147.136':'net2', '10.134.147.137':'net3'}  #host ip list##HC##
        self.duration = -1

    
    def transfer2dict(self, logline, starttime):
        '''
        transfer the rawlogline to formatted dict
        '''
        row = {}
        parts = logline.split()
        row['src'] = int(parts[0][-1]) + 1       #source of this flow 
        time = parts[1]                          #the time of this logline
        row['rtime'] = timestamp(time) - timestamp(starttime)   #the relative time of first flow
        dst =  self.hosts.index(self.host_ip[parts[2].split(':')[0]]) + 1       #destination of this flow ##HC##
        row['dst'] =  dst  
        row['size'] = int(parts[3].split('=')[-1])  #the size of this flow
        fid = int(parts[4])     #stage id 
        
        #get the shuffle stage of this flow
        if fid in self.sdict.keys():
            row['sid'] = self.sdict[fid]
        else:
            self.count += 1
            self.sdict[fid] = self.count
            row['sid'] = self.sdict[fid]
        
        row['mid'] = int(parts[5])  #mapper id
        return row

    def log2metrics(self):
        dirpath = self.dirpath + self.appID + '/'       ##get the log file folder ##HC##
        self.data = pd.DataFrame(columns=['src', 'rtime', 'dst', 'size', 'sid', 'mid'])
        
        #To get the relationship between hostname and executor index
        driverlogpath = dirpath + 'driverlog'
        with open(driverlogpath,'r') as driverlog:
            line = driverlog.readline()
            while line :

                if 'Executor added:' in line:
                    host = line.split('(')[1].split(':')[0]
                    self.hosts.append(host)
                elif 'org.apache.spark.network.netty.NettyBlockTransferService' in line:
                    break
                line = driverlog.readline()           

        executorlogpath = dirpath + 'executor_0'    #the master log file name ##HC##
        with open(executorlogpath,'r') as exelog:
            timeline = exelog.readline()
            while timeline:
                ## to find the starttime of this application ##HC##
                if 'Started daemon with process name' in timeline:
                    starttime = timeline.split()[1]
                    break
                timeline = exelog.readline()

        #calculate the duration of this application
        jsonlogpath = dirpath + 'metrics.json'
        with open(jsonlogpath,'r') as jsonlog:
            metrics_dict = json.load(jsonlog)
        self.duration = metrics_dict['appDuration']

        logpath = dirpath + 'logs.txt'      #the formatted logs file name ##HC##

        #transfer the formatted logs to dict
        with open(logpath,'r') as log:
            rawtransfer = log.readline()
            while rawtransfer:
                row = self.transfer2dict(rawtransfer,starttime)
  
                self.data = self.data.append([row], ignore_index=True)
  
                rawtransfer = log.readline()
        
        self.data.to_csv('logs.csv')

    def metrics2flows(self):
        self.flows = [] 
        sp = random.randint(1000,65535)
        dp = random.randint(1000,65535)
        #record every flow info
        for (src,dst,sid), group in self.data.groupby(['src', 'dst', 'sid']):
            while group.size > 0:
                flow = Flow()
                flow.src = src
                flow.dst = dst
                flow.sid = sid
                flow.sp = sp    ##HC##
                flow.dp = dp    ##hc##
                tag = self.appID + str(sid)
                flow.tag = tag.split('-')[1]+tag.split('-')[2]
                flow.sid = sid
                flow.size = 0
                midlist = []                         
                for index, row in group.iterrows():
                    if row['mid'] not in midlist:
                        midlist.append(row['mid'])
                        flow.size += row['size']
                        if row['rtime'] < flow.rtime:
                            flow.rtime = row['rtime']
                        group = group.drop(index)
                self.flows.append(flow)
        print(len(self.flows))
        return self.flows


if __name__ == "__main__":

    tf = Transfer('Z:/spark_executor_logs_and_traces/ScalaPageRank/3/', 'app-20190404194031-0030')
    tf.log2metrics()
    tf.metrics2flows()

        

# def log2flows(dirpath):
#     data = log2metrics(dirpath)
#     flows = metrics2flowinfo(data)
#     return flows


