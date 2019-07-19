import os
#from logexecutor import get_last_line
# path = 'Z:/spark_executor_logs_and_traces/ScalaPageRank/3/app-20190401203500-1549/'
# # for fn in os.listdir(path):
# #     print (fn)
# print(os.listdir(path))

# path = 'Z:/spark_executor_logs_and_traces/ScalaPageRank/3/app-20190401203500-1549/executor_0'


# starttime = get_last_line(path).decode('utf8').split()[1]
# print(starttime)

# class  A:
#     def __init__(self):
#         self._a = 0
#         self.__b = 1
#         self.c = 2
#     def print(self):
#         print(self.__b)
# class B(A):
#     def print(self):
#         self.a = A._a
#         print (self.a)

# a = A()
# a.__b = 3
# print(a.__b)
# a.print()
# b = B()
# b.print()
# executors = []
# host_ip = {'10.134.147.135':'net1', '10.134.147.136':'net2', '10.134.147.137':'net3'}

# with open('Z:/spark_executor_logs_and_traces/ScalaPageRank/3/app-20190604181411-0072/driverlog','r') as f :
#     line = f.readline()
#     while line :

#         if 'Executor added:' in line:
#             #print(line)
#             host = line.split('(')[1].split(':')[0]
#             executors.append(host)
#         elif 'org.apache.spark.network.netty.NettyBlockTransferService' in line:
#             break
#         line = f.readline()

# print(executors.index('net1'))

s = 'app-20190604181411-0072' 
a = s.split('-')[1]+s.split('-')[2]
print(a)