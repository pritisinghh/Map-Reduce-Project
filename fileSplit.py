from config import InpDirPath
import os

def dataSplit(nMappers):
    countFiles=0
    filenames=[]
    for f in os.listdir(InpDirPath):
        if os.path.isfile(os.path.join(InpDirPath, f)):
            countFiles+=1
            filenames.append(InpDirPath+"/"+f)
    
    if countFiles<nMappers:
        nMappers=countFiles
    distCount=countFiles//nMappers
    remaining=countFiles % distCount
    
    group=[distCount]*nMappers
    group[-1]=distCount+remaining
    groupfilenames={}

    start=0
    for i in range(len(group)):
        end=start+group[i]
        groupfilenames[i]=filenames[start:end]
        start=end
        
    return groupfilenames