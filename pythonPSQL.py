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

def insertIntoTables(tableName, insertValues, insertColumn, returningValue):

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
        INSERT INTO  course(Tittle) VALUES('Math') RETURNING CourseNo;
    """
    curs = conn.cursor()

    curs.execute(commands, (tableName, insertColumn,
                 insertValues, returningValue))
    print(curs.fetchall())
    conn.close()


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
    print(row[0])
    row[0] = row[0].__str__().replace("('","")
    row[0] = row[0].__str__().replace("',)","")
    commands = "DROP TABLE {0};".format(row[0])
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



def printOptions():
    print("c ---for creating a new Table")
    print("i ---for inserting values to a table")
    print("q ---for quering a table")
    print("e ---for exiting Creator")

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