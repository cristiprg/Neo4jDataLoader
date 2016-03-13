import os
import shutil
import getopt
import sys

dbPath = ""
facebookData = ""

tmpDataFolder = "tmpData"
personsFilename = os.path.join(tmpDataFolder, "persons.csv") # nodes
friendshipsFilename = os.path.join(tmpDataFolder,"friendships.csv") # nodes
relationshipsFilename = os.path.join(tmpDataFolder,"relationships.csv") # relationships - or edges

def createCSVFiles():
    """
    1) Creates two csv's for nodes: persons and friendships
    2) Creates a csv for relationshis
    """
    # create the three files
    if not os.path.exists(tmpDataFolder):
        os.makedirs(tmpDataFolder)
    personsFile = open(personsFilename, "w+")
    friendshipsFile = open(friendshipsFilename, "w+")
    relationshipsFile = open(relationshipsFilename, "w+")

    # write headers
    personsFile.write("personID:ID\n")
    friendshipsFile.write("friendshipID:ID,timestamp\n")
    relationshipsFile.write(":START_ID,:END_ID,:TYPE\n")

    # read the datafile
    with open(facebookData) as f:
        lines = f.readlines()

    persons = set()
    index = 0
    # ignore the first line - the headers
    for line in lines[1:]:
        split_line = line[0:-1].split(",")
        start_node = split_line[0]
        end_node = split_line[1]
        timestamp = split_line[3]

        # add nodes to set
        persons.add(int(start_node))
        persons.add(int(end_node))

        # write friendship to file
        currentFriendshipID = "f" + str(index)
        friendshipsFile.write(currentFriendshipID + "," + timestamp + "\n")

        # write relationship to file
        relationshipsFile.write(start_node+","+currentFriendshipID + ",HAS\n")
        relationshipsFile.write(end_node+","+currentFriendshipID + ",HAS\n")

        index += 1

    friendshipsFile.close()
    relationshipsFile.close()

    # write the nodes to file
    for person in sorted(persons):
        personsFile.write(str(person) + "\n")
    personsFile.close()


def restartDB():
    shutil.rmtree(dbPath)
    os.makedirs(dbPath)

def importDB():
    neo4jCommand = '\
    neo4j-import \
    --into %(dbPath)s \
    --nodes:Person %(personsFilename)s \
    --nodes:Friendship %(friendshipsFilename)s \
    --relationships %(relationshipsFilename)s \
    --delimiter ","' % {"dbPath": dbPath, "personsFilename": personsFilename, "friendshipsFilename":friendshipsFilename, "relationshipsFilename": relationshipsFilename}

    os.system(neo4jCommand)

def usage():
    print """
Usage: loadNeo4jData.py --dbPath=<pathToNeo4jDB> --facebookData=<pathToFacebookDataSet>

Creates new Neo4j data base at dbPath using the provided facebookData. Remember to stop Neo4j before executing this.
    """

if __name__ == "__main__":
    # parse command-line args
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "dbPath=", "facebookData="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt == '--dbPath':
            dbPath = arg
        elif opt == '--facebookData':
            facebookData = arg

    if dbPath == "" or facebookData == "":
        usage()
        sys.exit(1)

    # go
    print "Creating csv files ..."
    createCSVFiles()

    print "Deleting database ..."
    restartDB()

    print "Bulk-loading in Neo4j ..."
    importDB()
