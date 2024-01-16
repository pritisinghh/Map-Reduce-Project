import socket
import threading
from config import masterPort,mapperStartPort,reducerStartPort
from fileSplit import dataSplit
from mapper import mapper_word_count,mapper_inverted_index,mapperSend,runMapper
from reducer import sendToReducer
from config import OutDirPath
import collections
import json
import os
from signal import signal,SIGPIPE,SIG_DFL
signal(SIGPIPE,SIG_DFL)


def init_reducer(reducerNo,port):
    sockRed = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    reducerAddress = ('', port)
    sockRed.bind(reducerAddress)
    sockRed.listen(9)
    reducer_output = {}
    filename=''
    print(f"Reducer keeps listening on port {port}")
    while True:
        # print(f"Reducer listening on port {port}")
        connection, client_address = sockRed.accept()
        msg=connection.recv(5024)
        k , v = msg.decode('utf-8').split(',',1)
        if appName.lower()=="wordcount":
            if k in reducer_output:
                reducer_output[k] +=int(v) 
            else:
                reducer_output[k]=int(v)

        if appName.lower()=="invertedindex":
            v=eval(v)
            if k in reducer_output:
                tempDict=reducer_output.get(k)
                tempDict.update(v)
                reducer_output[k]=tempDict
            else:
                reducer_output[k]=v
        # print(len(reducer_output))
        # dir_name = OutDirPath+f"\reducer{port}"
        # if not os.path.exists(dir_name):
        #     os.makedirs(dir_name)

        # os.chdir(dir_name)
        filename = "/foo/bar/baz.txt"
        filename=OutDirPath+"/"+f"redcuer{port}"+f"/reducer{port}.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        out_file = open(filename, "w")
        json.dump(reducer_output, out_file,indent=6)
        print("Reducer stores data in output....")
        out_file.close()
     
def init_mapper(appName,port,nReducers):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    mapperAddress = ('', port)
    sock.bind(mapperAddress)
    serverName=socket.gethostbyname(socket.gethostname())
    senderSocket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    senderSocket.connect((serverName,masterPort))
    sock.listen(5)
    while True:
        print(f"Mapper started listening on port {port}")
        connection, client_address = sock.accept()
        msg=connection.recv(4096)
        msg=msg.decode('utf-8')
        filenames = msg.split(',')
        if appName.lower()=="wordcount":
            output=runMapper( filenames, mapper_word_count)
        if appName.lower()=="invertedindex":
            output=runMapper( filenames, mapper_inverted_index)

        print(f"mapper at {port} sending back ack")
        senderSocket.send('Mapper done'.encode('utf-8'))

        print(f"Mapper on {port} sending data to reducer ")
        sendToReducer(appName,output,nReducers)
        break

def startMaster(port , nMappers,appName):
    serverSocket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)
    serverSocket.bind(('',port))
    
    mapperProc=[]
    for i in range(nMappers):
        t=threading.Thread(target=(init_mapper),args=(appName,mapperStartPort+i,nReducers,))
        mapperProc.append(t)

    for i in range(nMappers):
        mapperProc[i].start()

    serverSocket.listen(5)
    ack=0
    while True:
        connection,address=serverSocket.accept()
        ack+=1
        if ack==nMappers:
            print("All ack from mapper received")
            break

def main(nMappers,nReducers,appName):
    filenames=dataSplit(nMappers)
    print("Distributing files to all mappers")

    if nMappers!=len(filenames):
        print(f"Specified number of mappers were more, actual no required was {len(filenames)}") 
        nMappers=len(filenames)

    print("Starting master node")
    master_mapper_process = threading.Thread(target = (startMaster) , args=(masterPort , nMappers,appName))
    master_mapper_process.start()

  
    mapperfile=[]
    for i in range(nMappers):
        t=threading.Thread(target=(mapperSend),args=(i,filenames[i]))
        mapperfile.append(t)
    
    for i in range(nMappers):
        mapperfile[i].start()

    for i in range(nMappers):
        mapperfile[i].join()

    reducerProc=[]
    for i in range(nReducers):
        t=threading.Thread(target=(init_reducer),args=(i,reducerStartPort+i,))
        reducerProc.append(t)

    for i in range(nReducers):
        reducerProc[i].start()
    
       
if __name__ == '__main__':
    appName=input("enter application name , WordCount or InvertedIndex: ")
    nMappers=int(input("enter number of mappers:"))
    nReducers=int(input("enter number od reducers:"))
    main(nMappers,nReducers,appName)