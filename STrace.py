import os
from logexecutor import Flow, Transfer

class Executor(object):
    'The executor of traces from varies sources'
    def __init__(self, path):
        self.Logpath = Logpath
    
    def print(self):
        appsinfo = []
        applist =  os.listdir(self.Logpath)
        print('There are total %d application logs'%len(applist))
        for appID in applist:
            print(appID+'...')
            path = self.Logpath + appID + '/'
            filelist = os.listdir(path)
            # if 'flowtrace' in filelist:
            #     continue
            # else:
            if 'logs.txt' not in filelist:
                continue 
            else:
                app = {}    
                tf = Transfer(Logpath,appID)
                tf.log2metrics()
                flows = tf.metrics2flows()
                app['appID'] = appID
                app['duration'] = tf.duration 
                app['flows'] = flows
                appsinfo.append(app)
                flowtrace = path + 'flowtrace'
                with open(flowtrace,'w') as tracefile:
                    line = appID + ' ' + str(tf.duration) + '\n'
                    tracefile.write(line)
                    for flow in flows:
                        line = str(flow.rtime)+ ' ' + str(flow.src)+ ' ' + str(flow.dst)+ ' ' + str              (flow.size)+ ' ' + str(flow.sp)+ ' ' + str(flow.dp)+ ' ' + str(flow.tag)+ '\n'
                        tracefile.write(line)
            print('Done...')
        
class Scheduler(object):
    'The scheduler is responsible for generating trace sequences according to configurations'
    
    pass

class Generator(object):
    'The generator to produce the specified type trace'
    pass

if __name__ == "__main__":
    Logpath = '/mnt/nas/spark_executor_logs_and_traces/ScalaPageRank/3/'
    ect = Executor(Logpath)
    ect.print()


    