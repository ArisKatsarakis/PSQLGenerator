import psycopg2


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
        conn = psycopg2.connect(
            database='suppliers',
            host='localhost',
            user="postgres",
            password='root',
            port='5433'

        )

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
        print("Executing Query: ")
        print(commands)
        curs = conn.cursor()
        curs.execute(commands, tableName)
        if  curs.rowcount == 0:
            raise ModuleNotFoundError
        row = curs.fetchall()
        print(row)
        conn.close()
    except ModuleNotFoundError:
        print("'The Table doen\'t exist '")
        return ModuleNotFoundError
        
    return curs.rowcount


if __name__ == '__main__':
    # insertIntoTables('COURSE',"Math","Title",'CourseNo')
    print('PSQL AUTO TABLE GENERATOR')
    print('which Table Columns do you want to See')
    tableName = input()
    columns = showColumns(str(tableName))
    while columns != ModuleNotFoundError:
        tableName = input()
        columns = showColumns(str(tableName))
        
