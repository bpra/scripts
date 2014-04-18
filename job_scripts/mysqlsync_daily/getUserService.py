import pymongo, datetime, time
from pymongo import Connection
# TODO: Error handling
# TODO: Connect to Vertica and get delta key instead of using local file
# TODO: Use a general function for all keys instead of repetitive code for each key
loc = "/home/deploy/tablecopy/deltaDt"
# Timestamp file (ideally should be read from Vertica)
deltaDtFile = open(loc, 'r')
deltaDt = datetime.datetime.fromtimestamp(float(deltaDtFile.readline()))

# Connect to the database
connection = Connection('mongo01.analytics.renttherunway.it')
db = connection.userservice

# Get the collection
userService = db.UserProfile

# Initialize this to get everything
#deltaDt = datetime.datetime(1980, 01, 25, 00)


# Query for changes
# Equivalent Mongo query
# db.UserProfile.find({modified : {$gt : ISODate("2013-02-25") }}, {userId:1, modified:1, email:1, heightInches:1, usStandardSize:1, bodyType:1, canonicalUserType:1})
#for user in userService.find({"modified" : {"$gt": deltaDt}}, {"userId":1, "modified":1, "email":1, "heightInches":1, "usStandardSize":1, "bodyType":1, "canonicalUserType":1}).sort("modified"):
for user in userService.find({"modified" : {"$gt": deltaDt}}, {"userId":1, "modified":1, "email":1, "zipcode":1, "heightInches":1, "weightPounds":1, "usStandardSize":1, "bodyType":1, "bustSize":1, "canonicalUserType":1}):
  mdt = user["modified"]
  if ("heightInches" in user.keys()):
    ht = str(user["heightInches"])
  else:
    ht = ""

  if ("usStandardSize" in user.keys()):
    sz = str(user["usStandardSize"])
  else:
    sz = ""
  
  if ("email" in user.keys()):
    em = "" #str(user["email"])
  else:
    em = ""
  
  if ("zipcode" in user.keys()):
    zip = str(user["zipcode"])
  else:
    zip = ""
  
  if ("weightPounds" in user.keys()):
    wt = str(user["weightPounds"])
  else:
    wt = ""
  
  if ("bodyType" in user.keys()):
    bt = str(user["bodyType"])
  else:
    bt = ""
  
  if ("bustSize" in user.keys()):
    bs = str(user["bustSize"])
  else:
    bs = ""

  print str(user["userId"]) + ", " + em + ", " + ht + ", " + sz + ", " + wt + ", " + bt + ", " + bs + ", " + zip + ", ",  mdt

# Update Timestamp file
deltaDtFile.close()
deltaDtFile = open(loc, 'w')
deltaDtFile.write(str(time.mktime(mdt.timetuple())))

