#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 15:23:38 2017

@author: louis
"""

#final1_2
import time,urllib.request, json, sqlite3

conn = sqlite3.connect('Tweets_fina_Database.db')
c = conn.cursor()
wFD = urllib.request.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")
#f = open('final.text','w')

c.execute('DROP TABLE IF EXISTS Tweets');
c.execute('DROP TABLE IF EXISTS USER');
c.execute('DROP TABLE IF EXISTS GEO');

createTable = '''CREATE TABLE Tweets (
                 ID          NUMBER(20),
                 Created_At  DATE,
                 Text        CHAR(140),
                 Source VARCHAR(200) DEFAULT NULL,
                 In_Reply_to_User_ID NUMBER(20),
                 In_Reply_to_Screen_Name VARCHAR(60),
                 In_Reply_to_Status_ID NUMBER(20),
                 Retweet_Count NUMBER(10),
                 Contributors  VARCHAR(200),
                 user_id NUMBER(20),
                 CONSTRAINT Tweets_PK  PRIMARY KEY (id)
              );'''
createUserTable = '''
    CREATE TABLE USER(
    ID NUMBER(20) PRIMARY KEY,
    Name VARCHAR(50),
    SCREEN_NAME VARCHAR(50),
    DESCRIPTION VARCHAR(200),
    FRIENDS_COUNT NUMBER(10),
    Foreign Key (ID) REFERENCES TWEETS(USER_ID)
    );
'''
createGeoTable = '''
    CREATE TABLE GEO(
    Tweet_ID NUMBER(20) PRIMARY KEY,
    TYPE VARCHAR(20),
    LONGITUDE NUMBER(9,6),
    LATITUDE NUMBER(9,6),
    FOREIGN KEY (Tweet_ID) REFERENCES TWEETS(ID)
    ); 
'''
c.execute(createTable)
c.execute(createUserTable)
c.execute(createGeoTable)

def loadTweets(wFD):
        count = 0
       
        key_list = []
        key_listUser = []
        
        line = (wFD.readline()).decode('utf8')
       
        batchRows = 50
        
        batchedInserts = []
        batchedInsertsUser = []
        batchedInsertsGeo = []
    
        tweetKeys = ['id_str','created_at','text','source',
                         'in_reply_to_user_id', 'in_reply_to_screen_name', 
                         'in_reply_to_status_id', 'retweet_count', 'contributors','user']
        userKeys = ['id','name','screen_name','description','friends_count']
        
        
        while(line):
            tDict = json.loads(line)
            
            newRow = [] # hold individual values of to-be-inserted row
            newRow2 = []
            newRow3 = []
             
            if tDict['id_str'] in key_list:
               line = (wFD.readline()).decode('utf8')
               count += 1
               if (count % 10000 == 0):
                   print('Total load =',count)
               continue
            key_list.append(tDict['id_str'])
            #populate geo table
            
            
           
            if not(tDict['geo'] == None or tDict['geo'] in ['',[],'null']):
                    newRow3.append(tDict['id_str'])
                    newRow3.append(tDict['geo']['type'])
                    newRow3.append(tDict['geo']['coordinates'][0])
                    newRow3.append(tDict['geo']['coordinates'][1])
            
            #POPULATE TWEETS AND USERS       
            for key in tweetKeys:
            #when key is user we have to take care of it
                if key == 'user':
                    #if nothing in user, then just appned None in Tweets
                    if tDict[key] in ['',[],'null']:
                        newRow.append(None)
                    else:
                        for key2 in userKeys:
                            #if key2 is id, we have to take care of it
                            if key2 == 'id':
                                #there are 3 situations:nothing,duplicate,good
                                #for nothing we should add none to table Tweets and user
                                if tDict[key][key2] in ['',[],'null']:
                                    newRow2.append(None)
                                    newRow.append(None)
                                #for duplicate we should add it to table Tweets and ignore all the rest 
                                elif tDict[key][key2] in key_listUser:
                                    newRow.append(tDict[key][key2])
                                    break
                                else:
                                    #if it's good .add to both tables and add it to list
                                    newRow2.append(tDict[key][key2])
                                    newRow.append(tDict[key][key2])
                                    key_listUser.append(tDict[key][key2])
                                continue
                            #otherwise, just follow normal precedure
                            if tDict[key][key2] in ['',[],'null']:
                                newRow2.append(None)
                            else : 
                                newRow2.append(tDict[key][key2])
                    continue
            
                if tDict[key] in ['',[],'null']:
                    newRow.append(None)
                else:
                    newRow.append(tDict[key])
            #ADD each row to a list        
            batchedInserts.append(newRow)
            if newRow2 != []:
                batchedInsertsUser.append(newRow2)
            if newRow3 != []:
                batchedInsertsGeo.append(newRow3)
            
            line = (wFD.readline()).decode('utf8')
            #when we have 50 data we add them to table
            if len(batchedInserts) >= batchRows or len(line) == 0:
                c.executemany('INSERT INTO Tweets VALUES(?,?,?,?,?,?,?,?,?,?)', batchedInserts)
                batchedInserts = []
            if len(batchedInsertsUser) >= batchRows or len(line) == 0:
                c.executemany('INSERT INTO USER VALUES(?,?,?,?,?)', batchedInsertsUser)        
                batchedInsertsUser = []
            if len(batchedInsertsGeo) >= batchRows or len(line) == 0:
                c.executemany('INSERT INTO GEO VALUES(?,?,?,?)', batchedInsertsGeo)        
                batchedInsertsGeo = []
            #we end when we traverse 5000 tweets
            count += 1
            if (count % 10000 == 0):
                print('Total load =',count)
  
start = time.time()
loadTweets( wFD)
end   = time.time()
#46.5--50000
print('Table Tweets has',c.execute('SELECT COUNT(*) FROM Tweets').fetchall()[0],'rows')
print('Table USER has',c.execute('SELECT COUNT(*) FROM USER').fetchall()[0],'rows')
print('Table Geo has',c.execute('SELECT COUNT(*) FROM GEO').fetchall()[0],'rows')

print("loadTweets took ", (end-start), ' seconds.')
# =============================================================================
# 
# # =============================================================================
# # FindTweet = '''
# #     SELECT * FROM Tweets
# #     WHERE ID LIKE '%44%'
# #     OR ID LIKE '%77%';
# # '''
# # print(c.execute(FindTweet).fetchall())
# # =============================================================================
# 
# FindUnique = '''
#     SELECT COUNT(DISTINCT(In_Reply_to_User_ID)) FROM Tweets;
# '''
# print(c.execute(FindUnique).fetchall()[0])
# 
# FindAvg = '''
#     SELECT AVG(LONGITUDE), AVG(LATITUDE)
#     FROM GEO G
#     JOIN Tweets T
#     ON G.ID = T.ID
#     JOIN USER U
#     ON U.ID = T.user_id
#     WHERE LONGITUDE IS NOT NULL
#     AND LATITUDE IS NOT NULL
#     Group by U.Name
# '''
# 
# print(c.execute(FindAvg).fetchall())
# =============================================================================


wFD.close()
#f.close()
c.close()
conn.commit()
conn.close()