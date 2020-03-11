import os
import pandas as pd
import shutil as sh

#DATABASE CLASS
class DataBase:
    tableName = ''
    backupName = ''
    table = {}
    columnA = columnB = columnC = ""
    safeRegym = False
    lastError = 0
    set_ops = set()
    cache_id_ops = {}
    def __init__(self, name, columnA = "", columnB = "", columnC = ""):
        self.tableName = name + '.csv'
        self.columnA = columnA
        self.columnB = columnB
        self.columnC = columnC
        if os.path.exists('./'+self.tableName):
            self.LoadFromCSV()
        else:
            df = pd.DataFrame(columns=['ID', self.columnA, self.columnB, self.columnC])
            df.to_csv(self.tableName, index=False, header=True)
        self.set_ops = set()
        self.cache_id_ops = {}
        
    def ClearDB(self):
        self.SaveBackUp()
        del self.table
        self.table = {}

    def cache(self, ID, data):
        id_operation = 0
        for i in data.keys():
            qu= '*'.join([str(i),str(data[i])])
            if qu in self.set_ops:
                self.cache_id_ops[qu].add(ID)
            else:
                self.cache_id_ops[qu] = set([ID])
                self.set_ops.add(qu)

    def cacheDelete(self, ID):
        data = self.table[ID]
        for i in data.keys():
            qu = '*'.join([str(i),str(data[i])])
            self.cache_id_ops[qu].remove(ID)
            if (len(self.cache_id_ops[qu]) == 0):
                self.set_ops.remove(qu)
            
    def cacheReturn(self, column, value):
        try:
            return self.cache_id_ops['*'.join([column,value])]
        except:
            return '$'

    def LoadFromCSV(self):
        df = 0
        try:
            df = pd.read_csv(self.tableName)
        except:
            lastError = 1
            print('no file')
            return 0
        self.table = {}
        for index, row in df.iterrows():
            self.table[str(row['ID'])] = {self.columnA: row[self.columnA], self.columnB: row[self.columnB], self.columnC: row[self.columnC]}
            self.cache(str(row['ID']), self.table[str(row['ID'])])
            
    def SaveDBtoCSV(self):
        cs = [[],[],[],[]]
        for i in  self.table.keys():
            cs[0].append(i)
            cs[1].append(self.table[i][self.columnA])
            cs[2].append(self.table[i][self.columnB])
            cs[3].append(self.table[i][self.columnC])
        df = pd.DataFrame({'ID': cs[0], self.columnA:cs[1], self.columnB:cs[2], self.columnC:cs[3]})
        try:
            df.to_csv(self.tableName, index=False)
        except:
            lastError = 1
            print("error in file")
            return 0

    def AppendRow(self, ID, columnA, columnB, columnC):
        if ID == '$':
            self.lastError = 1
            return
        if ID not in self.table:
            self.table[ID] = {self.columnA: columnA, self.columnB: columnB, self.columnC: columnC}
            self.cache(ID, self.table[ID])
        else:
            self.lastError = 1

    def FindRows(self, column, value):
        ret = self.cacheReturn(column,value)
        if ret == '$':
            return -1
        else:
            return ret

    def DeleteByID(self, ID):
        try:
            self.cacheDelete(ID)
            del self.table[ID]
        except:
            self.lastError = 1
        
    def UpdateRow(self, ID, changes):
        try:
            self.table[ID]
        except:
            self.lastError = 1
            return -1
        self.cacheDelete(ID)
        for k in changes.keys():
            self.table[ID][k] = changes[k]
        self.cache(ID, self.table[ID])
    
    def SaveBackUp(self):
        try:
            self.backupName = '$'+self.tableName[:-4]+'.csv'
            sh.copy(self.tableName,self.backupName)
        except:
            self.lastError = 2

    def LoadFromBackUp(self):
        tmp = self.tableName
        self.tableName = self.backupName
        if os.path.exists(self.tableName):
            self.LoadFromCSV()
        self.tableName = tmp

#TEST ON PERFOMANCE:
if __name__=="__main__":
    #############################
    #       UNNIT TEST          #
    #############################
	print("START TESTING:")
    db2 = DataBase("kl","a","b","c")
    db2.AppendRow(1,"qwe","qwe","dga")
    if (db2.table[1] != {"a": "qwe","b": "qwe","c": "dga"}):
        print("ERROR!!!1")
    else:
        print("OK")
    db2.ClearDB()
    if (db2.table != {}):
        print("ERROR!!!2")
    else:
        print("OK")
    db2.AppendRow(1,"qwe","qwe","dga")
    db2.UpdateRow(1,{"a":"test"})
    if (db2.table[1] != {"a": "test","b": "qwe","c": "dga"}):
        print("ERROR!!!3")
    else:
        print("OK")
    if (db2.FindRows("a","qwe") != set()):
        print("ERROR!!!4", db2.FindRows("a","qwe"))
    else:
        print("OK")
    if (db2.FindRows("a","test") != set([1])):
        print("ERROR!!!5", db2.FindRows("a","test"))
    else:
        print("OK")
    db2.DeleteByID(1)
    if (db2.table != {}):
        print("ERROR!!!6")
    else:
        print("OK")

    ##############################
    #        SYSTEM TESTING      #
    ##############################

    #test.csv
    db3 = DataBase("test", "a", "b", "c")
    db3.AppendRow('1',"qwe","qwe","dga")
    db3.SaveDBtoCSV()
    db3.ClearDB()
    db3.LoadFromCSV()
    if (db3.table['1'] != {"a": "qwe","b": "qwe","c": "dga"}):
        print("ERROR!!!1")
    else:
        print("OK")
    
    ###############################
    #       TEST PERFOMANCE       #
    ###############################
    
    import timeit
    from time import time
    from matplotlib import pyplot as plt
    import math
    complexity = 10
    db = DataBase("kl", "a","b","c")
    def gtime(testCase):
        testTime = []
        for i in range (0, complexity):
            for j in range(0, 1000*i):    #HERE STEP OF ITERATTION DEFAULT=1000*COMPLEXITY
                db.AppendRow(j, "a", "b", "c")
            tic = time()
            testCase(i)
            toc = time()
            db.ClearDB()
            testTime.append(toc-tic)
        return testTime
   
    
    def testCase1(j):
        db.AppendRow(1000*j+1, "a", "b", "c") # APPEND ROW IN DB
            
    def testCase2(j):
        db.DeleteByID(100*j/2)# DELETE ROW FROM DB

    def testCase3(j):
        db.FindRows("c", "c")# FIND ROWS FROM DB

    def testCase4(j):
        db.UpdateRow(100*j/2, {"a": "qwter"}) #CHANGE ROW IN DB
           
    plt.figure(figsize=(9, 5))
    plt.suptitle('Operations With Data Base Complexity (msec / rows)')
    
    def gtest(testCase, num, name):
        array = gtime(testCase)
        plt.subplot(140+num)
        plt.plot([1+1000*i for i in range (0, complexity)], array, c='r')
        print(num, "time table: ", array)
        plt
        plt.legend([name])

    gtest(testCase1, 1, 'AppendRow')
    gtest(testCase2, 2, 'DeleteByID')
    gtest(testCase3, 3, 'FindRows')
    gtest(testCase4, 4, 'UpdateRow')
    plt.show()
	print('END TESTS')
    
    

