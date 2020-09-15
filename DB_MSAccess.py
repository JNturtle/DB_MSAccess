from pyodbc import connect

class DB():
    def __init__(self, path, SHOWTEXT = True):
        """
        path: 資料庫的完整位置
        """
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'    
            r'DBQ=' + path + ';'
            )
        self.connectState = False
        self.cnxn = None
        self.crsr = None
        self.SHOWTEXT = SHOWTEXT
        try:
            self.cnxn = connect(conn_str)
            self.crsr = self.cnxn.cursor()
            self.connectState = True
            if self.SHOWTEXT: print("資料庫連接", path, "成功！")
        except:
            if self.SHOWTEXT: print("資料庫連接", path, "失敗！")
            self.cnxn = None
        return None

    def checkExist(self, tableName, condition, value, condition2 = '', value2 ='', condition3='', value3 ='', condition4='', value4 =''):
        """
        查詢tableName中的condition欄，是否存在value，返回布林值。最多可以輸入四組條件
        """
        if condition4 != '' and value4 !='': #四重檢索
            checkPoint = self.crsr.execute("select "+condition+" from "+tableName+" where "+condition+"=?"+' and '+condition2+'=? and '+condition3+"=? and "+condition4+"=?",value,value2,value3,value4).fetchall() 
        elif condition3 != '' and value3 !='': #三重檢索
            checkPoint = self.crsr.execute("select "+condition+" from "+tableName+" where "+condition+"=?"+' and '+condition2+'=? and '+condition3+"=?",value, value2, value3).fetchall()
        elif condition2 != '' and value2 !='': #二重檢索
            checkPoint = self.crsr.execute("select "+condition+" from "+tableName+" where "+condition+"=?"+' and '+condition2+"=?",value, value2).fetchall()
        else: # 單檢索
            checkPoint = self.crsr.execute("select "+condition+" from "+tableName+" where "+condition+"=?",value).fetchall()
        # 以數量確認是否存在  
        return len(checkPoint) != 0

    def createTable(self, newTableName, sourceTable = "0000"):
        """
        如果表格不存在，從 table_source 複製結構並輸出成 new_table_name
        """
        if newTableName in tableNames:
            if self.SHOWTEXT :print(newTableName, "已經存在於資料庫")
            exist = True
        else:
            self.crsr.execute( "select * into " + newTableName + " from " + sourceTable + " where 1=0 ")
            self.cnxn.commit()  
            if self.SHOWTEXT :print("已經創立", newTableName, "到資料庫")
        return None

    def getColumnsName(self, tableName):
        return [name for row in self.crsr.columns(tableName)]

    def WriteTableWithDataList(self, table_name, DataList):  
        """一次寫入多行數據"""
        SQL = self._insertSQL(table_name)
        self.crsr.executemany(SQL, DataList)     
        self.cnxn.commit()  
        return True

    @property
    def tableNames(self):
        return [names for row in self.crsr.tables()]

    def _insertSQL(self):
        Columns = "" 
        values = ""
        for each in self.getColumnsName(table_name):
            Columns += str(each)+","
            values += r"?,"        
        SQL = "insert into " + table_name + "(" + Columns[:-1] + ") values (" + values[:-1] + ")"
        return SQL

if __name__ == "__main__":
    import os   
    db = DB(os.path.join(os.getcwd(), 'test.accdb'))

    