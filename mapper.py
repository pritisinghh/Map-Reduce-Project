from config import mapperStartPort
import collections, re
import socket

def mapperSend(i,files):
    port=mapperStartPort+i
    serverName=socket.gethostbyname(socket.gethostname())
    clientSocket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    clientSocket.connect((serverName,port))
    files=','.join(files)
    clientSocket.send(files.encode('utf-8'))

def runMapper(files,mapperFunc):
    return mapperFunc(files)

def mapper_word_count(data):
    dataDict= collections.defaultdict(int)
    # data = [data]
    for i in range(len(data)):
        f=open(data[i],'r')
        files=f.read()
        # print(words)
        for w in files.split():
            w = re.sub("[^A-Za-z]" , '' , w).lower()
            dataDict[w]+=1
    return dataDict
   
                
def mapper_inverted_index(data):    
    invDict = {}
    for i in range(len(data)):
        f=open(data[i],'r')
        files=f.read()
        # print(words)
        for w in files.split():
            w=re.sub("[^A-Za-z]",'',w).lower()
            if w!='':
                if w in invDict:
                    tempDict=invDict.get(w)
                    if data[i] in tempDict:
                        tempDict[data[i]]=tempDict.get(data[i])+1
                    else:
                        tempDict[data[i]]=1
                    invDict[w]=tempDict
                else:
                    invDict[w]={data[i]:1}
    return invDict   
       