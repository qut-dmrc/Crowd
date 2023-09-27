import sqlite3 as sql
import pandas as pd
from crowd.togbq import *
from crowd.fields import history_schema, main_data_schema, media_schema, expanded_links_schema

conn = sql.connect('db_name')
sqlc = conn.cursor()

tablename="table_name"

## Get all job_ids 
# df = pd.read_sql_query("SELECT _searchTermLink,min(date),max(date),_jobEntryDateTime FROM "+tablename+ " Group By _jobEntryDateTime" , conn)
# print(df)

# all the job ids to be pusehd to BQ(crowdtangle)
job_ids = [
    # "true"
]
bq_key = "dmrc-data-26d311ddd0bf.json" #sevice account key to push to BQ
# # #store to csv
for job_id in job_ids:
    mode = 'w'
    header = True
    chunksize = 50000
    for chunk in pd.read_sql_query("SELECT * FROM "+tablename, conn, chunksize=chunksize):
    # for chunk in pd.read_sql_query("SELECT * FROM "+tablename+" WHERE _jobEntryDateTime=\""+job_id+"\"", conn, chunksize=chunksize):
        chunk = chunk[chunk.columns.difference(['_pushedToBQ','_jobEntryDateTime','_searchTermLink'])]
        chunk = chunk[chunk['platformId'] != 'platformId']
        chunk.to_csv("temp.csv", index=False)
        append_to_bq(bq_key, "crowdtangle."+tablename, "temp.csv", schema=main_data_schema)
    
    for chunk in pd.read_sql_query("SELECT * FROM "+tablename+"_media", conn, chunksize=chunksize):
    # for chunk in pd.read_sql_query("SELECT * FROM "+tablename+"_media WHERE _jobEntryDateTime=\""+job_id+"\"", conn, chunksize=chunksize):
        chunk = chunk[chunk.columns.difference(['_pushedToBQ','_jobEntryDateTime','_searchTermLink'])]
        chunk = chunk[chunk['platformId'] != 'platformId']
        chunk.to_csv("temp.csv", index=False)
        append_to_bq(bq_key, "crowdtangle."+tablename+"_media", "temp.csv", schema=media_schema)

    for chunk in pd.read_sql_query("SELECT * FROM "+tablename+"_expandedLinks", conn, chunksize=chunksize):
    # for chunk in pd.read_sql_query("SELECT * FROM "+tablename+"_expandedLinks WHERE _jobEntryDateTime=\""+job_id+"\"", conn, chunksize=chunksize):
        chunk = chunk[chunk.columns.difference(['_pushedToBQ','_jobEntryDateTime','_searchTermLink'])]
        chunk = chunk[chunk['platformId'] != 'platformId']
        chunk.to_csv("temp.csv", index=False)
        append_to_bq(bq_key, "crowdtangle."+tablename+"_expandedLinks", "temp.csv", schema=expanded_links_schema)

    # count = 0
    for chunk in pd.read_sql_query("SELECT * FROM "+tablename+"_history", conn, chunksize=chunksize):
    # for chunk in pd.read_sql_query("SELECT * FROM "+tablename+"_history WHERE _jobEntryDateTime=\""+job_id+"\"", conn, chunksize=chunksize):
        # count += 50000
        # if count > 123200000:
        chunk = chunk[chunk.columns.difference(['_pushedToBQ','_jobEntryDateTime','_searchTermLink'])]
        chunk = chunk[chunk['platformId'] != 'platformId']
        chunk.to_csv("temp.csv", index=False)
        append_to_bq(bq_key, "crowdtangle."+tablename+"_history", "temp.csv", schema=history_schema)
        # else:
        #     print("skip")


    # sqlc.execute("DELETE FROM "+tablename+" WHERE _jobEntryDateTime = '"+job_id+"'")
    # sqlc.execute("DELETE FROM "+tablename+"_media WHERE _jobEntryDateTime = '"+job_id+"'")
    # sqlc.execute("DELETE FROM "+tablename+"_expandedLinks WHERE _jobEntryDateTime = '"+job_id+"'")
    # sqlc.execute("DELETE FROM "+tablename+"_history WHERE _jobEntryDateTime = '"+job_id+"'")
    conn.commit()
