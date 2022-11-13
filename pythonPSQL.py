import psycopg2

tablesCount  = 0
from pprint import pprint


def getNewConnection():
    return psycopg2.connect(
        database='suppliers',
        host='localhost',
        user="postgres",
        password='root',
        port='5433'

    )


def createTables():
    # Commands for our tables
    # COURSE (CourseNo, Title, Year, ClassSize, InstructorId)
    # STUDENT (StudentId, Name, Semester)
    # ENROLL (StudentId, CourseNo, Year)
    # INSTRUCTOR (InstructorId, Name, OfficeNo)
    commands = (
        """
        CREATE TABLE COURSE (
            CourseNo SERIAL PRIMARY KEY,
            Tittle VARCHAR(255) NOT NULL,
            Year INTEGER,
            ClassSize INTEGER,
            InstructorId SERIAL
        )
        """,
        """ CREATE TABLE STUDENT (
                StudentId SERIAL PRIMARY KEY,
                Name VARCHAR(255) NOT NULL,
                Semester INTEGER
                )
        """,
        """
        CREATE TABLE ENROLL (
            StudentId SERIAL,
            CourseNo SERIAL,
            Year INTEGER
        )
        """,
        """
        CREATE TABLE INSTRUCTOR (
                InstructorId SERIAL PRIMARY KEY,
                Name VARCHAR(255),
                OfficeNo Integer
        )
        """)

    try:
        conn = getNewConnection()

        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#
# First of All declares the Types of the columns in the new SQL Table
# then creates the sql Query dynamically
# executes and commits the changes in the DB 
# #
def createNewTable(tableName,tableValues):
    tableValuesString = """"""
    conn =  getNewConnection()
    for value in tableValues:
        print(value.__str__() + " is an INTEGER/VARCHAR/SERIAL ? ")
        valueType = input()
        tableValuesString = tableValuesString +" " + value + " " + valueType + ","
    print(tableValuesString)
    curs = conn.cursor()    
    commands = """
        CREATE TABLE {0} (
            {1}
        )
    
    """.format(tableName, tableValuesString)
    print(commands)
    
    index =  commands.rfind(',')
    commands = commands[:index] + commands[index+1:]
    curs.execute(commands)
    conn.commit()
    conn.close()       

def insertIntoTables(tableName):

    conn = psycopg2.connect(
        database='suppliers',
        host='localhost',
        user="postgres",
        password='root',
        port='5433'

    )
    # commands = """
    #     INSERT INTO %s(%s)
    #          VALUES(%s) RETURNING %s ;
    # """
    commands = """
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = '{0}'
    """.format(tableName)
    
    curs = conn.cursor()
    curs.execute(commands)
    row = curs.fetchone()
    print("-------Values-------")
    variables = """"""
    values = """"""
    
    # Dynamic Building of the Query String
    while row is not None:
        print(row[0] + "= ? " )
        variables = variables + row[0] + ","
        if(row[1] == "varchar"):
            values = values  + "'"+ input() + "'"
        else:
            values = values  + input() + ", "        
        row = curs.fetchone()
    
    index = values.rfind(',')
    values = values[:index] + values[index+1:]
    index = variables.__str__().rfind(",")
    variables = variables[:index] + variables[index+1:] 
    insertQuery = """
        INSERT INTO {0} ({1}) VALUES({2})
    """.format(tableName,variables, values)
    print(insertQuery)
    curs.execute(insertQuery)
    conn.commit()
    conn.close()
    printTableValues(tableName)
    

def printTableValues(tableName):
    conn = getNewConnection()
    curs = conn.cursor()
    commands  = """
        SELECT * FROM {0};
    """.format(tableName)
    curs.execute(commands)
    count = curs.rowcount
    print("Results for {tableName} are: "+ count.__str__())
    row = curs.fetchone()
    while row is not None:
        print(row)
        row = curs.fetchone()
    
    
def showColumns(tableName):
    try:

        conn = psycopg2.connect(
            database='suppliers',
            host='localhost',
            user="postgres",
            password='root',
            port='5433'

        )

        commands = """
            select column_name,data_type from information_schema.columns where 
            table_name = '{0}'
        """.format(tableName)
        # print("Executing Query: ")
        # print(commands)
        curs = conn.cursor()
        curs.execute(commands, tableName)
        if curs.rowcount == 0:
            raise ModuleNotFoundError
        row = curs.fetchall()
        for column in row:
            column  = column.__str__().replace("(","")
            column  = column.__str__().replace(")","")
            column  = column.__str__().replace("'","")
            pprint(column,width=30,indent=10,depth=10)
            
        conn.close()
    except ModuleNotFoundError:
        print("'The Table doen\'t exist '")
        return ModuleNotFoundError

    return curs.rowcount


def dropAllTables():
    conn = getNewConnection()
    curs = conn.cursor()
    # show all tables
    commands = """
     select tab.table_name FROM information_schema.tables tab WHERE table_schema = 'public'
    """
    curs.execute(commands)
    row = curs.fetchall()
    for tab in row:
        print("Drooping Table " + tab[0].__str__() + " !!!")
        commands = "DROP TABLE {0};".format(tab[0])
        curs.execute(commands)
        print(commands)

        
    #Always Use commit after each execute that post Changes
    conn.commit()
    conn.close()

def getTablesCount():

    conn = getNewConnection()
    curs = conn.cursor()
    psql = """
        SELECT table_name FROM information_schema.tables where table_schema = 'public'
    """
    curs.execute(psql)
    row = curs.rowcount
    if row == 0:
        return 0
    else:
        return row

def printAllTables():
    conn = getNewConnection()
    curs = conn.cursor()
    commands = """
        select tab.table_name from information_schema.tables tab where table_schema = 'public'
    """    
    curs.execute(commands)

    for row in curs.fetchall():
        print("------Table----------")
        row = row.__str__().replace("('","")
        row = row.__str__().replace("',)","")
        print(row.__str__())
        print("------Values----------")
        showColumns(row)

def printAllTableValues():
    conn = getNewConnection()
    curs = conn.cursor()
    tableQuery = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public'
    """
    curs.execute(tableQuery)
    #in Tables we put the names of the TAbles
    tables = []
    row = curs.fetchone()
    while row is not None:
        tables.append(row[0])
        row = curs.fetchone()
    
    print(tables)
    for tab in tables:
        print("TABLE: " + tab.__str__())
        print("""+++++++++++++++Columns++++++++++++++++""")
        showColumns(tab)
        print("""++++++++++++++++Values++++++++++++++++""")
        selectQuery = """
            SELECT * FROM {0}
        """.format(tab)
        curs.execute(selectQuery)
        val = curs.fetchone()
        while val is not None:
            print(val)
            val = curs.fetchone()
            
    conn.close()
def printOptions():
    print("c ---for creating a new Table")
    print("i ---for inserting values to a table")
    print("q ---for quering a table")
    print("p ---print all table values")
    print("e ---for exiting Creator")
    
def queryTable():
    conn = getNewConnection()
    curs = conn.cursor()
    query = input("Please place a query ")
    curs.execute(query)
    row = curs.rowcount
    if row == 0:
        conn.close()
        return
    else:
        row = curs.fetchone()
        while row is not None:
            print(row)
            row = curs.fetchone()
        
        conn.commit()
        conn.close()
        return


if __name__ == '__main__':
    # insertIntoTables('COURSE',"Math","Title",'CourseNo')
    operationChar = "n"
    print('PSQL AUTO TABLE GENERATOR')
    print("These Are your Tables")
    tablesCount = getTablesCount()
    if tablesCount == 0 :
        print("There are No Tables in DB ")
        print('Lets Insert Some in Our DB')
    else:
        printAllTables()
    while operationChar != "e":
        printOptions()
        operationChar = input('Give an Option \n')
        match operationChar:
            case "c":
                print("Give A Table Name: ")
                tableName = input()
                print("Give All Values included in the Table")
                print("e -- if you want to stop adding values")
                value = """"""
                values = []
                while value != """e""":
                    value = input()
                    if value == """e""":
                        break
                    else:
                        values.append(value)
                createNewTable(tableName, values)
                printAllTables()
            case "i":
                printAllTables()
                print("Choose Table You want to Insert Values To:")
                tableName = input()
                insertIntoTables(tableName)
            case "q":
                printAllTableValues()
                queryTable()
            case "p":
                tablesCount = getTablesCount()
                if tablesCount==0:
                    print('There are no tables to print ')
                else:
                    printAllTables()
                    tableName = input("Insert the Table you want to print \n ")
                    printTableValues(tableName)
    dropAllTables()