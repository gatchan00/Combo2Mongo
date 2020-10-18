import os
import re
import json
import tokenize

from pymongo import MongoClient, UpdateOne

from combo2mongo_cfg import mongoConfig, rootPath, checkpointFile, batchSize, storeSource



patternLineaAsString = "[^\r\n:@a-zA-Z0-9.-_]*([^\r\n:]+):([^\r\n:]+).*"
mailPatternAsString = "([^\r\n:]+@[^\r\n:]+)"
patternValidTextAsString = "[a-zA-Z0-9.\\/<>+*=&%$\-,@|# ;ñÑáéíóúÁÉÍÓÚÄËÏÖÜäëïöü?¿¡!]"

pendingFiles = []
#PROCESS
patternLinea = re.compile(patternLineaAsString)
patternMail = re.compile(mailPatternAsString)
patternValidText = re.compile(patternValidTextAsString)
def createMongoConnection():
    client = MongoClient(mongoConfig['host'], mongoConfig['port'])
    db = client[mongoConfig['db']]
    mongoCollection = db[mongoConfig['collection']]
    return mongoCollection

def procesaFichero(filePath):
    print(filePath)
    tempFile = tokenize.open(filePath)
    fileEncoding = tempFile.encoding
    tempFile.close()
    with open(filePath, 'r', encoding=fileEncoding, errors="backslashreplace") as f:
        for linea in f:
            try:
                matcheo = patternLinea.match(linea)
                matcheoMail = patternMail.search(linea)
                if matcheo and matcheoMail:
                    userId = matcheoMail.group(1)
                    campos = linea.replace("\r", "").replace("\n", "").split(":")
                    #userPass = matcheo.group(2)
                    camposFinales = []
                    for elem in campos:
                        if elem != userId:
                            camposFinales.append(elem)
                    for campo in camposFinales:
                        if patternValidText.match(campo)==None:
                            camposFinales.remove(campo)
                    #print({'_id': userId}, {'$addToSet': {'pass': campos}})
                    #mongoCollection.update_one({'_id': userId}, {'$addToSet': {'pass': userPass}}, upsert=True)
                    yield UpdateOne({'_id': userId}, {'$addToSet': {'data': {'$each': camposFinales}}}, upsert=True)
                    if storeSource:
                        yield UpdateOne({'_id': userId}, {'$addToSet': {'sources': filePath}}, upsert=True)

            except:
                print("ERROR IN LINE: "+linea)
                continue

def processPath(path, mongoCollection, processedFiles):
    print('Processing '+path)
    for currPath in os.listdir(path):
        fullPath = os.path.join(path, currPath)
        if os.path.isdir(fullPath):
            for e in processPath(fullPath, mongoCollection, processedFiles):
                yield e
        elif os.path.isfile(fullPath):
            if fullPath not in processedFiles:
                for e in procesaFichero(fullPath):
                    yield e
                pendingFiles.append(fullPath)
            else:
                print("File <"+fullPath+"> skipped (already processes on another run)")

def bucleMongo(mongoCollection, processedFiles):
    mongoOperations = []
    totalOperations = 0
    for operation in processPath(rootPath, mongoCollection, processedFiles):
        mongoOperations.append(operation)
        totalOperations += 1
        if totalOperations % batchSize == 0:
            if storeSource:
                print("Hemos acabado con " + str(totalOperations/2))
            else:
                print("Hemos acabado con "+str(totalOperations))
            mongoCollection.bulk_write(mongoOperations)
            with open(checkpointFile, 'a') as saveCheckpointFile:
                for processedFile in pendingFiles:
                    saveCheckpointFile.write(processedFile)
                    saveCheckpointFile.write("\n")
                saveCheckpointFile.flush()
                pendingFiles.clear()
            mongoOperations = []
    mongoCollection.bulk_write(mongoOperations)
    mongoOperations = []

#MAIN
if __name__ == "__main__":
    mongoCollection = createMongoConnection()
    mongoCollection.drop()

    processedFiles = [checkpointFile]
    try:
        with open(checkpointFile, 'r') as f:
            for linea in f:
                linea = linea.replace("\r", "").replace("\n", "")
                processedFiles.append(linea)
    except:
        print("no checkpoints file <"+checkpointFile+">, a new one will be generated")
    bucleMongo(mongoCollection, processedFiles)

