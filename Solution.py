from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        Files = "CREATE TABLE Files(FileID INTEGER PRIMARY KEY NOT NULL ," \
                "FileType TEXT NOT NULL ," \
                "SizeNeeded INTEGER NOT NULL" \
                ",UNIQUE(FileID)," \
                "CHECK(FileID>0)," \
                "CHECK ( SizeNeeded>=0 ));"

        Disks = "CREATE TABLE Disks(" \
                "DiskID INTEGER PRIMARY KEY  NOT NULL," \
                " Company TEXT  NOT NULL," \
                "Speed INTEGER NOT NULL " \
                ",FreeSpace INTEGER NOT NULL," \
                "DiskCost INTEGER NOT NULL," \
                "UNIQUE(DiskID)," \
                "CHECK(DiskID>0)," \
                "CHECK(Speed>0)," \
                "CHECK(DiskCost>0)," \
                "CHECK(FreeSpace>=0)); "

        RAMs = "CREATE TABLE RAMs(" \
               "RAMID INTEGER PRIMARY KEY NOT NULL ," \
               " RAMSize integer NOT NULL ," \
               "RAMCompany TEXT NOT NULL," \
               "check (RAMID>0)," \
               "UNIQUE (RAMID)," \
               "CHECK ( RAMSize>0));"

        FilesandDisks = "CREATE TABLE FilesANDDisks(" \
                        "DiskID  INTEGER NOT NULL  CHECK ( DiskID > 0)," \
                        "FileID INTEGER NOT NULL CHECK (FILEID > 0)," \
                        "FOREIGN KEY ( DiskID) REFERENCES Disks(DiskID) ON DELETE CASCADE ON UPDATE CASCADE," \
                        "FOREIGN KEY (FileID) REFERENCES Files(FileID) ON DELETE CASCADE ON UPDATE CASCADE," \
                        " UNIQUE (DiskID, FileID));  "

        RAMsandDisks = "CREATE TABLE RAMSANDDisks" \
                       " (DiskID INTEGER NOT NULL CHECK (DiskID > 0)," \
                       " RAMID INTEGER NOT NULL CHECK (RAMID > 0 )," \
                       " FOREIGN KEY (DiskID) REFERENCES Disks(DiskID) ON DELETE CASCADE ON UPDATE CASCADE," \
                       " FOREIGN KEY (RAMID) REFERENCES RAMS(RAMID) ON DELETE CASCADE ON UPDATE CASCADE," \
                       " UNIQUE  (DiskID,RAMID)) ;"

        FilesSizeOnDiskView = "CREATE VIEW FilesSizeOnDiskView AS (" \
                              " SELECT Disks.DiskID ,Files.FileID, Files.SizeNeeded " \
                              "FROM Disks, Files,FilesandDisks " \
                              "WHERE Disks.DiskID=FilesandDisks.DiskID " \
                              "AND Files.FileID=FilesandDisks.FileID); "

        RAMsSizeOnDiskView = "CREATE VIEW RAMsSizeOnDiskView AS (" \
                             " SELECT Disks.DiskID,RAMs.RAMID, RAMs.RAMsize " \
                             "FROM Disks,RAMsandDisks, RAMs " \
                             "WHERE Disks.DiskID=RAMsandDisks.DiskID " \
                             "AND RAMs.RAMID=RAMsandDisks.RAMID); "

        FilesCostView = "CREATE VIEW FilesCostView AS ( " \
                        "SELECT Disks.DiskID,Files.FileID, Files.SizeNeeded ,Files.FileType, Disks.DiskCost " \
                        "FROM Disks, Files,FilesandDisks " \
                        "WHERE Disks.DiskID=FilesandDisks.DiskID AND" \
                        " Files.FileID=FilesandDisks.FileID); "

        RAMsCompanyOnDiskView = "CREATE VIEW RAMsCompanyOnDiskView AS (" \
                                " SELECT Disks.DiskID,RAMs.RAMID, RAMs.RAMCompany, Disks.Company " \
                                "FROM Disks,RAMsandDisks,RAMs " \
                                "WHERE Disks.DiskID=RAMsandDisks.DiskID AND" \
                                " RAMs.RAMID=RAMsandDisks.RAMID);"

        # temp1 = "CREATE VIEW temp1 AS (SELECT Disks.DiskID, COUNT(*) AS cunt FROM Disks,Files WHERE Files.SizeNeeded <= Disks.FreeSpace GROUP BY Disks.DiskID" \
        #                  "UNION " \
        #                  "SELECT Disk.DiskID, 0 AS Ccunt FROM Disks WHERE Disks.DiskID IN (" \
        #                  "SELECT Disks.DiskID AS did FROM Disks EXCEPT SELECT Disks.DiskID FROM Disks, Files WHERE Disks.FreeSpace>=Files.SizeNeeded GROUP BY Disks.DiskID)" \
        #                  "GROUP BY  Disks.DiskID);"

        FilesCountDisk = "CREATE VIEW FilesCountDisk AS (SELECT Disks.DiskID, COUNT(*) AS cunt FROM Disks,Files WHERE " \
                         " Files.SizeNeeded <= Disks.FreeSpace GROUP BY Disks.DiskID " \
                         "UNION " \
                         "SELECT Disks.DiskID, 0 AS cunt FROM Disks WHERE Disks.DiskID IN ( " \
                         "SELECT Disks.DiskID AS did FROM Disks EXCEPT SELECT Disks.DiskID FROM Disks, Files WHERE " \
                         "Disks.FreeSpace>=Files.SizeNeeded GROUP BY Disks.DiskID) " \
                         "GROUP BY  Disks.DiskID);"



        # NotRunningFilesView = "CREATE VIEW NotRunningFilesView AS (" \
        #                         " SELECT Disks.DiskID, Disks.Speed,0 AS amount FROM (" \
        #                       " SELECT DiskId, Speed FROM Disks EXCEPT SELECT DiskID) " \
        #                         "FROM Disks,RAMsandDisks,RAMs " \
        #                         "WHERE Disks.DiskID=RAMsandDisks.DiskID AND" \
        #                         " RAMs.RAMID=RAMsandDisks.RAMID);"
        # # RunningFilesView=

        conn.execute(
            Files + Disks + RAMs + FilesandDisks + RAMsandDisks + FilesSizeOnDiskView + RAMsSizeOnDiskView + FilesCostView + RAMsCompanyOnDiskView + FilesCountDisk)
    except Exception as e:
        conn.rollback()
        print(e)

    except DatabaseException as e:
        conn.rollback()
        print(e)
    finally:
        conn.commit()
        conn.close()


def clearTables():
    conn = None
    error = Status.OK
    try:
        conn = Connector.DBConnector()
        # CASCADE is used when deleting\updating the tables, causes an error therefore it was removed
        # it means that the child data is either updated or deleted
        # we should be using drop table if exists instead of drop table,
        # to avoid any errors, in case a table does not exist
        conn.execute("BEGIN;"
                     " DELETE  FROM Files  ;"
                     " DELETE  FROM Disks   ;"
                     " DELETE  FROM RAMs    ;"
                     " DELETE  FROM FilesandDisks   ;"
                     " DELETE  FROM RAMsandDisks   ;"
                     "COMMIT;")
    #     CASCADE at the end of the statement
    #     if the above doesn't work, we should try: DELETE * FROM <TABLE_NAME> CASCADE
    except DatabaseException as e:
        # conn.rollback()
        print(e)
        error = DatabaseException.UNKNOWN_ERROR
    except Exception as e:
        # conn.rollback()
        print(e)
        error = Status.ERROR
    finally:
        conn.commit()  # needed to save the changes that were made to the table
        conn.close()
        return error


# down here, we can drop all tables in a one row kareem
def dropTables():
    conn = None
    error = Status.OK
    try:
        conn = Connector.DBConnector()
        # CASCADE is used when deleting\updating the tables,
        # it means that the child data is either updated or deleted
        # we should be using drop table if exists instead of drop table,
        # to avoid any errors, in case a table does not exist
        conn.execute("BEGIN;"
                     "DROP TABLE IF EXISTS Files CASCADE ;"
                     "DROP TABLE IF EXISTS Disks CASCADE ;"
                     "DROP TABLE IF EXISTS RAMs CASCADE ;"
                     "DROP TABLE IF EXISTS FilesandDisks CASCADE ;"
                     "DROP TABLE IF EXISTS RAMsandDisks CASCADE ;"
                     "COMMIT;")
    #     CASCADE was at the end of each statement
    except DatabaseException as e:
        # conn.rollback()
        print(e)
        error = DatabaseException.UNKNOWN_ERROR
    except Exception as e:
        # conn.rollback()
        print(e)
        error = Status.ERROR
    finally:
        conn.commit()  # needed to save the changes that were made to the table
        conn.close()
        return error


def addFile(file: File) -> Status:
    conn = None
    res = Status.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Files(FileID,FileType,SizeNeeded) VALUES({id},{ftype},{sizeneeded}); ").format \
            (id=sql.Literal(file.getFileID()), ftype=sql.Literal(file.getType()),
             sizeneeded=sql.Literal(file.getSize()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
        res = Status.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        res = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        res = Status.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        res = Status.ERROR
    except DatabaseException.UNKNOWN_ERROR as e:
        conn.rollback()
        print(e)
    except Exception as e:
        conn.rollback()
        print(e)
        res = Status.ERROR
    finally:
        conn.close()
        return res


def getFileByID(fileID: int) -> File:
    conn = None
    requested = File().badFile()
    error = Status.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Files WHERE FileID={id}").format(id=sql.Literal(fileID))
        rows_affected, result = conn.execute(query)
        conn.commit()
        if rows_affected == 1:
            requested.setFileID(result.rows[0][0])
            requested.setSize(result.rows[0][2])
            requested.setType(result.rows[0][1])
        else:
            requested = File.badFile()
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
        requested = File.badFile()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        requested = File.badFile()
    except Exception as e:
        conn.rollback()
        print(e)
        requested = File.badFile()
    finally:
        conn.close()
        return requested


# kareem do here
def deleteFile(file: File) -> Status:
    conn = None
    error = Status.OK
    try:
        conn = Connector.DBConnector()
        toDo = sql.SQL("BEGIN;"
                       " UPDATE Disks"
                       " SET FreeSpace=FreeSpace+{toDelSize}"
                       "WHERE DiskID IN "
                       "(SELECT DiskID "
                       "FROM FilesANDDisks  "
                       "WHERE FileID= {toDelID}); "
                       "DELETE FROM Files WHERE FileID = {toDelID}; "
                       # "DELETE FROM FilesandDisks WHERE FileID = {toDelID}; "
                       "COMMIT;").format(toDelID=sql.Literal(file.getFileID()), toDelSize=sql.Literal(file.getSize()))
        conn.execute(toDo)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
        error = Status.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        error = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        error = Status.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    except DatabaseException.UNKNOWN_ERROR as e:
        conn.rollback()
        print(e)
        error = DatabaseException.UNKNOWN_ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    finally:
        conn.close()
        return error


def addDisk(disk: Disk) -> Status:
    conn = None
    res = Status.OK
    try:
        conn = Connector.DBConnector()
        toDo = sql.SQL("INSERT INTO Disks VALUES({DiskID}, {Company}, {Speed}, {FreeSpace}, {diskCost})").format(
            DiskID=sql.Literal(disk.getDiskID()),
            Company=sql.Literal(disk.getCompany()),
            Speed=sql.Literal(disk.getSpeed()),
            FreeSpace=sql.Literal(disk.getFreeSpace()),
            diskCost=sql.Literal(disk.getCost()))
        conn.execute(toDo)
        conn.commit()
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
        res = Status.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        res = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        res = Status.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        res = Status.ERROR
    except DatabaseException.UNKNOWN_ERROR as e:
        conn.rollback()
        print(e)
        res = DatabaseException.UNKNOWN_ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        res = Status.ERROR
    finally:
        conn.close()
        return res


def getDiskByID(diskID: int) -> Disk:
    conn = None
    requested = Disk()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Disks WHERE DiskID={id}").format(id=sql.Literal(diskID))
        rows_affected, result = conn.execute(query)
        conn.commit()
        if rows_affected == 1:
            requested.setDiskID(result.rows[0][0])
            requested.setCompany(result.rows[0][1])
            requested.setSpeed(result.rows[0][2])
            requested.setFreeSpace(result.rows[0][3])
            requested.setCost(result.rows[0][4])
        else:
            requested = Disk.badDisk()
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
        requested = Disk.badDisk()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        requested = Disk.badDisk()
    except Exception as e:
        conn.rollback()
        print(e)
        requested = Disk.badDisk()
    finally:
        conn.close()
        return requested


# query1= sql.SQL("DELETE FROM Disks WHERE DiskId={id}")
#        query2= sql.SQL("DELETE FROM FilesandDisks WHERE DiskID={id}")
#       query3= sql.SQL("DELETE FROM RAMsandDisks WHERE DiskID={id}")
#      query = sql.SQL("BEGIN;" + query3 + query2 + query1 + "COMMIT;").format(sql.Literal(diskID))
#     result = conn.execute(query)
def deleteDisk(diskID: int) -> Status:
    conn = None
    error = Status.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disks WHERE DiskId={id}").format(id=sql.Literal(diskID))
        result, _ = conn.execute(query)
        conn.commit()
        if result == 0:
            error = Status.NOT_EXISTS
    except DatabaseException as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    finally:
        conn.close()
        return error


def addRAM(ram: RAM) -> Status:
    conn = None
    error = Status.OK

    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RAMs(RAMID,RAMSize,RAmCompany) VALUES({id},{Rsize},{Rcompany});").format \
            (id=sql.Literal(ram.getRamID()), Rsize=sql.Literal(ram.getSize()),
             Rcompany=sql.Literal(ram.getCompany()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
        error = Status.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        error = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        error = Status.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    except DatabaseException.UNKNOWN_ERROR as e:
        conn.rollback()
        print(e)
        error = DatabaseException.UNKNOWN_ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    finally:
        conn.close()
        return error


def getRAMByID(ramID: int) -> RAM:
    conn = None
    requested = RAM.badRAM()
    # error = Status.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM RAMs WHERE RAMID={id};").format(id=sql.Literal(ramID))
        rows_affected, result = conn.execute(query)
        conn.commit()
        if rows_affected == 1:
            requested.setRamID(result.rows[0][0])
            requested.setSize(result.rows[0][1])
            requested.setCompany(result.rows[0][2])
        else:
            requested = RAM.badRAM()
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
        requested = RAM.badRAM()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        requested = RAM.badRAM()
    except Exception as e:
        conn.rollback()
        print(e)
        requested = RAM.badRAM()
    finally:
        conn.close()
        return requested


def deleteRAM(ramID: int) -> Status:
    conn = None
    error = Status.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RAMs WHERE RAMID= {id};").format(id=sql.Literal(ramID))
        result, _ = conn.execute(query)
        conn.commit()
        if result == 0:
            error = Status.NOT_EXISTS
    except DatabaseException as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    finally:
        conn.close()
        return error


def addDiskAndFile(disk: Disk, file: File) -> Status:
    conn = None
    error = Status.OK
    try:
        conn = Connector.DBConnector()
        query1 = "INSERT INTO Disks VALUES({DiskID}, {Company}, {Speed}, {FreeSpace}, {diskCost});"
        query2 = "INSERT INTO Files VALUES({FileID}, {FileType}, {SizeNeeded});"
        query = sql.SQL("BEGIN;" + query1 + query2 + "COMMIT;").format(
            FileID=sql.Literal(file.getFileID()),
            FileType=sql.Literal(file.getType()),
            SizeNeeded=sql.Literal(file.getSize()),
            DiskID=sql.Literal(disk.getDiskID()),
            Company=sql.Literal(disk.getCompany()),
            Speed=sql.Literal(disk.getSpeed()),
            FreeSpace=sql.Literal(disk.getFreeSpace()),
            diskCost=sql.Literal(disk.getCost()))
        conn.execute(query)
        conn.commit()

    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        error = Status.ALREADY_EXISTS
    except DatabaseException as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    finally:
        conn.close()
        return error

    # kareem check


def addFileToDisk(file: File, diskID: int) -> Status:
    conn = None
    res = Status.OK

    try:
        conn = Connector.DBConnector()
        query1 = "INSERT INTO FilesandDisks VALUES ({DiskID},{FileID});"
        query2 = "UPDATE Disks SET FreeSpace = Disks.FreeSpace - (SELECT SizeNeeded FROM Files " \
                 "WHERE FileID={FileID} AND DiskID={DiskID}) WHERE DiskID = {DiskID};"
        query = sql.SQL("BEGIN;" + query1 + query2 + "COMMIT;").format(FileID=sql.Literal(file.getFileID()),
                                                                       DiskID=sql.Literal(diskID))
        # query1 = "INSERT INTO FilesandDisks VALUES ({DiskID},{FileID});" \
        #          " UPDATE Disks SET FreeSpace = Disks.FreeSpace - {neededsize} " \
        #          "WHERE DiskID = {DiskID}; "
        # query2 = "UPDATE Disks SET FreeSpace = Disks.FreeSpace - {neededsize} " \
        #          " WHERE DiskID = {DiskID};"
        # query = sql.SQL("BEGIN;" + query1 + "COMMIT;").format(FileID=sql.Literal(file.getFileID()),
        #                                                       DiskID=sql.Literal(diskID),
        #                                                       neededsize=sql.Literal(file.getSize()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        res = Status.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        res = Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
        res = Status.BAD_PARAMS
    except DatabaseException as e:
        conn.rollback()
        print(e)
        res = Status.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        res = Status.ERROR
    finally:
        conn.close()
        return res


def removeFileFromDisk(file: File, diskID: int) -> Status:
    conn = None
    res = Status.OK
    try:
        conn = Connector.DBConnector()
        query1 = "DELETE FROM FilesandDisks WHERE FileID={filedel} AND DiskID={diskdel};"
        query2 = "UPDATE Disks SET FreeSpace = Disks.FreeSpace + {neededsize} " \
                 " WHERE DiskID = {diskdel} AND {filedel} IN (SELECT FileID FROM FilesandDisks where DiskID={diskdel});"
        query = sql.SQL("BEGIN;" + query2 + query1 + "COMMIT;").format(filedel=sql.Literal(file.getFileID()),
                                                                       diskdel=sql.Literal(diskID),
                                                                       neededsize=sql.Literal(file.getSize()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        res = Status.OK
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        res = Status.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        res = Status.ERROR
    finally:
        conn.close()
        return res


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    conn = None
    rows_affected = 0
    error = Status.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RAMsAndDisks(DiskID, RAMID) "
                        "SELECT DiskID,RAMID FROM RAMs,Disks WHERE RAMID={RAMID} AND DiskID={DiskID}").format(
            RAMID=sql.Literal(ramID),
            DiskID=sql.Literal(diskID))
        rows_affected, _ = conn.execute(query)
        conn.commit()
        if rows_affected == 0:
            error = Status.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        error = Status.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        error = Status.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    finally:
        conn.close()
        return error


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    conn = None
    error = Status.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RAMsandDisks WHERE RAMID={ramid} AND DiskID={diskid}").format(
            ramid=sql.Literal(ramID), diskid=sql.Literal(diskID))
        rows_affected, _ = conn.execute(query)
        conn.commit()
        if rows_affected == 0:
            # conn.close()
            error = Status.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    except DatabaseException as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        error = Status.ERROR
    finally:
        conn.close()
        return error


# the table is set to not have null's ,no need to view working on a table with minimal info
def averageFileSizeOnDisk(diskID: int) -> float:
    conn = None
    row_affected = -1
    temp = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT AVG(SizeNeeded) FROM FilesSizeOnDiskView WHERE DiskID={id}").format(
            id=sql.Literal(diskID))
        row_affected, temp = conn.execute(query)
        conn.commit()
        if temp.rows[0][0] is not None and len(temp.rows) != 0:
            row_affected = temp.rows[0][0]
        else:
            row_affected = 0

    except DatabaseException as e:
        conn.rollback()
        print(e)
        row_affected = -1
    finally:
        conn.close()
        return row_affected


def diskTotalRAM(diskID: int) -> int:
    conn = None
    row_affected = -1
    temp = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT SUM (RAMsize) FROM RAMsSizeOnDiskView WHERE DiskID={id}").format(
            id=sql.Literal(diskID))
        row_affected, temp = conn.execute(query)
        conn.commit()
        if temp.rows[0][0] is not None and len(temp.rows) != 0:
            row_affected = temp.rows[0][0]
        else:
            row_affected = 0

    except DatabaseException as e:
        conn.rollback()
        print(e)
        row_affected = -1
    finally:
        conn.close()
        return row_affected


def getCostForType(type: str) -> int:
    conn = None
    rows_affected = 0
    result = 0
    error = -1
    try:
        conn = Connector.DBConnector()
        query = (
            "SELECT COALESCE(SUM(SizeNeeded*DiskCost),0) FROM FilesCostView WHERE FilesCostView.FileType = {FType}")
        Query = sql.SQL(query).format(FType=sql.Literal(type))
        rows_affected, result = conn.execute(Query)
        conn.commit()
        if len(result.rows) == 0 or result.rows[0][0] is None:
            # if result.rows[0][0] == None:
            error = 0
        else:
            error = result.rows[0][0]

    except DatabaseException as e:
        print(e)
        error = -1
    except Exception as e:
        print(e)
        error = -1
    finally:
        conn.close()
        return error


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT Files.FileID "
                        "FROM Files ,Disks "
                        "WHERE ((Files.SizeNeeded <= Disks.FreeSpace) "
                        "AND Disks.DiskID= {shitID} )"
                        " ORDER BY Files.FileID DESC LIMIT 5").format(shitID=sql.Literal(diskID))
        effected, temp = conn.execute(query)
        conn.commit()
        if temp.rows[0][0] is None or len(temp.rows) == 0:
            # if result.rows[0][0] == None:
            result = []
        else:
            result = [i[0] for i in temp.rows]

    except DatabaseException as e:
        conn.rollback()
        print(e)
        result = []
    except Exception as e:
        conn.rollback()
        print(e)
        result = []
    finally:
        conn.close()
        return result


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    result = []
    # "  Disks.DiskID={ID} AND "
    # "Files.SizeNeeded <= Disks.FreeSpace "
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT DISTINCT Files.FileID FROM Files "
            "WHERE "
            "Files.SizeNeeded <= (SELECT Disks.FreeSpace FROM Disks WHERE DiskID={ID}) "
            "AND (  ((SELECT (SUM(RAMsize)) FROM RAMsSizeOnDiskView WHERE RAMsSizeOnDiskView.Diskid={ID} )>= "
            "Files.SizeNeeded) "
            "  OR Files.SizeNeeded =0)"
            "  ORDER BY Files.FileID ASC LIMIT 5"). \
            format(ID=sql.Literal(diskID))
        effected, temp = conn.execute(query)
        conn.commit()
        if len(temp.rows) == 0:
            # if result.rows[0][0] == None:
            result = []
        else:
            result = [i[0] for i in temp.rows]
            # for i in range(0, effected + 1):
            #     result.append(temp.rows[i][0])
    except DatabaseException as e:
        conn.rollback()
        print(e)
        result = []
    except Exception as e:
        conn.rollback()
        print(e)
        result = []
    finally:
        conn.close()
        return result


def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    rows_affected = 0
    res = False
    #  OR (SELECT COUNT(*) FROM RAMsandDisks WHERE RAMsandDisks.DiskID={id})<1
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL(
            "SELECT DiskID FROM Disks WHERE DiskID={id} AND  ((SELECT COUNT(*) FROM RAMsCompanyOnDiskView WHERE DiskID = {id} AND (Company = "
            "RAMsCompanyOnDiskView.RAMCompany))= (SELECT COUNT(*) FROM RAMSandDisks WHERE RAMSandDisks.DiskID ={id} ))").format(
            id=sql.Literal(diskID))
        rows_affected, result = conn.execute(query1)
        if len(result.rows) != 0:
            # res = False
            # else:
            res = True
    except DatabaseException as e:
        conn.rollback()
        print(e)
        res = False
    except Exception as e:
        conn.rollback()
        print(e)
        res = False
    finally:
        conn.commit()
        return res


# kareem check
def getConflictingDisks() -> List[int]:
    conn = None
    rows_affected = 0
    TheList = []
    try:
        conn = Connector.DBConnector()
        q1 = sql.SQL("SELECT DISTINCT DiskID FROM FilesandDisks WHERE FileID IN ("
                     "SELECT FileID FROM (SELECT FileID,COUNT(*) AS num FROM FilesandDisks GROUP BY FileID) AS tmp " \
                     "WHERE tmp.num > 1) "
                     "ORDER BY DiskID ASC").format()
        rows_affected, result = conn.execute(q1)
        conn.commit()
        if len(result.rows) > 0:
            TheList = [i[0] for i in result.rows]
        else:
            TheList = []
            # for i in range(0, result.size()):
            #     TheList.append(result.rows[i][0])
            # return TheList
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        conn.rollback()
        TheList = []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
        TheList = []
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        TheList = []
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
        TheList = []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
        TheList = []
    except Exception as e:
        print(e)
        conn.rollback()
        TheList = []
    finally:
        conn.close()
        return TheList


# kareem check
def mostAvailableDisks() -> List[int]:
    conn = None
    rows_affected = 0
    TheList = []
    try:
        conn = Connector.DBConnector()
        # toDo = sql.SQL("SELECT * FROM").format()
        # q1 = sql.SQL("SELECT * FROM (SELECT * FROM RunningFilesView UNION SELECT * FROM NotRunningFilesView) AS foo ORDER BY amount DESC, Speed DESC, DiskID ASC LIMIT 5").format()
        # q1 = sql.SQL("SELECT * FROM RunningFilesView UNION SELECT * FROM NotRunningFilesView)"
        #              " AS foo ORDER BY amount DESC, Speed DESC, DiskID ASC LIMIT 5").format()
        q1 = sql.SQL("SELECT shit.DiskID FROM"
                     " (SELECT Disks.DiskID,Disks.Speed, FilesCountDisk.cunt"
                     " FROM Disks,FilesCountDisk"
                     " WHERE Disks.DiskID=FilesCountDisk.DiskID"
                     " ORDER BY FilesCountDisk.cunt DESC,"
                     " Disks.Speed DESC,"
                     "Disks.DiskID ASC)"
                     " AS shit "
                     "LIMIT 5").format()
        rows_affected, result = conn.execute(q1)
        conn.commit()
        if len(result.rows) == 0 or result.rows[0][0] is None:
            TheList = []
        else:
            TheList = [i[0] for i in result.rows]
            # for i in range(0, result.size()):
            #     TheList.append(result.rows[i][0])
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        conn.rollback()
        TheList = []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
        TheList = []
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        TheList = []
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
        TheList = []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
        TheList = []
    except Exception as e:
        print(e)
        conn.rollback()
        TheList = []
    finally:
        conn.close()
        return TheList


# kareem here
def getCloseFiles(fileID: int) -> List[int]:
    toRet = []
    conn = None
    try:
        conn = Connector.DBConnector()
        toDo = sql.SQL(" BEGIN; "
                       " SELECT FileID "
                       " FROM  Files tempFile"
                       " WHERE tempFile.FileID <> {currID} AND ("
                       " SELECT 2*COUNT(FileID) FROM FilesandDisks tmp2 WHERE tmp2.FileID = tempFILE.FileID AND EXISTS ( SELECT * FROM FilesandDisks tmp3 WHERE tmp3.FileID = {currID} AND tmp3.DiskID=tmp2.DiskID)) "
                       " >= ("
                       " SELECT  COUNT(FileID) FROM FilesandDisks tmp1 WHERE tmp1.FileID = {currID}) "
                       " ORDER BY FileID ASC LIMIT 10;").format(currID=sql.Literal(fileID))
        howMany, res = conn.execute(toDo)
        if len(res.rows) <= 0:
            # size = toRet.size()+1
            # for j in range(0, size):
            #     toRet.append(toRet.rows[j][0])
            # conn.close()
            toRet = []
            # return toRet
        else:
            # conn.close()
            toRet = [i[0] for i in res.rows]

    except Exception as e:
        print(e)
        conn.rollback()
        # conn.close()
        toRet = []
    finally:
        conn.close()
        return toRet
