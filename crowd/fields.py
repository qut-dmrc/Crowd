from typing import List
import datetime as dt
from google.cloud import bigquery


# # If comparisons will be required, this will need to change to an OrderedDict
# output_fields = {
#     "platformId": str,
#     "platform": str,
#     "date": dt.datetime,
#     "updated": dt.datetime,
#     "type": str,
#     "title": str,
#     "caption": str,
#     "description": str,
#     "message": str,
#     "expandedLinksOriginal": List[str],
#     "expandedLinksExpanded": List[str],
#     "link": str,
#     "postUrl": str,
#     "subscriberCount": int,
#     "score": float,
#     "mediaType": List[str],
#     "mediaUrl": List[str],
#     "mediaHeight": List[int],
#     "mediaWidth": List[int],
#     "mediaFull": List[str],
#     "actualLikeCount": int,
#     "actualShareCount": int,
#     "actualCommentCount": int,
#     "actualLoveCount": int,
#     "actualWowCount": int,
#     "actualHahaCount": int,
#     "actualSadCount": int,
#     "actualAngryCount": int,
#     "actualThankfulCount": int,
#     "actualCareCount": int,
#     "expectedLikeCount": int,
#     "expectedShareCount": int,
#     "expectedCommentCount": int,
#     "expectedLoveCount": int,
#     "expectedWowCount": int,
#     "expectedHahaCount": int,
#     "expectedSadCount": int,
#     "expectedAngryCount": int,
#     "expectedThankfulCount": int,
#     "expectedCareCount": int,
#     "accountId": str,
#     "accountName": str,
#     "accountHandle": str,
#     "accountProfileImage": str,
#     "accountSubscriberCount": int,
#     "accountUrl": str,
#     "accountPlatform": str,
#     "accountPlatformId": str,
#     "accountAccountType": str,
#     "accountPageAdminTopCountry": str,
#     "accountVerified": bool,
#     "imageText": str,
#     "videoLengthMS": int,
#     "liveVideoStatus": str,
#     "newId": str,
#     "id": str
# }

# output_history_fields = {
#     "historyActualTimestep": str,  # todo: check this
#     "historyActualDate": dt.datetime,  # todo: check this
#     "historyActualScore": int,
#     "historyActualLikeCount": int,
#     "historyActualShareCount": int,
#     "historyActualCommentCount": int,
#     "historyActualLoveCount": int,
#     "historyActualWowCount": int,
#     "historyActualHahaCount": int,
#     "historyActualSadCount": int,
#     "historyActualAngryCount": int,
#     "historyActualThankfulCount": int,
#     "historyActualCareCount": int,
#     "historyExpectedLikeCount": int,
#     "historyExpectedShareCount": int,
#     "historyExpectedCommentCount": int,
#     "historyExpectedLoveCount": int,
#     "historyExpectedWowCount": int,
#     "historyExpectedHahaCount": int,
#     "historyExpectedSadCount": int,
#     "historyExpectedAngryCount": int,
#     "historyExpectedThankfulCount": int,
#     "historyExpectedCareCount": int
# }
output_fields = [ "platformId", 
                    "platform", 
                    "date", 
                    "updated", 
                    "type", 
                    "title", #
                    "caption", #
                    "description", # 
                    "message", 
                    "expandedLinksOriginal",
                    "expandedLinksExpanded",
                    "link",
                    "postUrl",
                    "subscriberCount",
                    "score",
                    "mediaType",
                    "mediaUrl",
                    "mediaHeight",
                    "mediaWidth",
                    "mediaFull",
                    "actualLikeCount",
                    "actualShareCount",
                    "actualCommentCount",
                    "actualLoveCount",
                    "actualWowCount",
                    "actualHahaCount",
                    "actualSadCount",
                    "actualAngryCount",
                    "actualThankfulCount",
                    "actualCareCount",
                    "expectedLikeCount",
                    "expectedShareCount",
                    "expectedCommentCount",
                    "expectedLoveCount",
                    "expectedWowCount",
                    "expectedHahaCount",
                    "expectedSadCount",
                    "expectedAngryCount",
                    "expectedThankfulCount",
                    "expectedCareCount",
                    "accountId",
                    "accountName",
                    "accountHandle",
                    "accountProfileImage",
                    "accountSubscriberCount",
                    "accountUrl",
                    "accountPlatform",
                    "accountPlatformId",
                    "accountAccountType",
                    "accountPageAdminTopCountry", #
                    "accountVerified",
                    "imageText", #
                    "videoLengthMS", #
                    "liveVideoStatus", #
                    "newId",
                    "id"]

output_fields_schema = [ bigquery.SchemaField("platformId","STRING"), 
                    bigquery.SchemaField("platform","STRING"), 
                    bigquery.SchemaField("date","TIMESTAMP"), 
                    bigquery.SchemaField("updated","TIMESTAMP"), 
                    bigquery.SchemaField("type","STRING"), 
                    bigquery.SchemaField("title","STRING"), #
                    bigquery.SchemaField("caption","STRING"), #
                    bigquery.SchemaField("description","STRING"), # 
                    bigquery.SchemaField("message","STRING"), 
                    bigquery.SchemaField("expandedLinksOriginal","STRING"),
                    bigquery.SchemaField("expandedLinksExpanded","STRING"),
                    bigquery.SchemaField("link","STRING"),
                    bigquery.SchemaField("postUrl","STRING"),
                    bigquery.SchemaField("subscriberCount","INTEGER"),
                    bigquery.SchemaField("score","FLOAT"),
                    bigquery.SchemaField("mediaType","STRING"),
                    bigquery.SchemaField("mediaUrl","STRING"),
                    bigquery.SchemaField("mediaHeight","STRING"),
                    bigquery.SchemaField("mediaWidth","STRING"),
                    bigquery.SchemaField("mediaFull","STRING"),
                    bigquery.SchemaField("actualLikeCount","INTEGER"),
                    bigquery.SchemaField("actualShareCount","INTEGER"),
                    bigquery.SchemaField("actualCommentCount","INTEGER"),
                    bigquery.SchemaField("actualLoveCount","INTEGER"),
                    bigquery.SchemaField("actualWowCount","INTEGER"),
                    bigquery.SchemaField("actualHahaCount","INTEGER"),
                    bigquery.SchemaField("actualSadCount","INTEGER"),
                    bigquery.SchemaField("actualAngryCount","INTEGER"),
                    bigquery.SchemaField("actualThankfulCount","INTEGER"),
                    bigquery.SchemaField("actualCareCount","INTEGER"),
                    bigquery.SchemaField("expectedLikeCount","INTEGER"),
                    bigquery.SchemaField("expectedShareCount","INTEGER"),
                    bigquery.SchemaField("expectedCommentCount","INTEGER"),
                    bigquery.SchemaField("expectedLoveCount","INTEGER"),
                    bigquery.SchemaField("expectedWowCount","INTEGER"),
                    bigquery.SchemaField("expectedHahaCount","INTEGER"),
                    bigquery.SchemaField("expectedSadCount","INTEGER"),
                    bigquery.SchemaField("expectedAngryCount","INTEGER"),
                    bigquery.SchemaField("expectedThankfulCount","INTEGER"),
                    bigquery.SchemaField("expectedCareCount","INTEGER"),
                    bigquery.SchemaField("accountId","INTEGER"),
                    bigquery.SchemaField("accountName","STRING"),
                    bigquery.SchemaField("accountHandle","STRING"),
                    bigquery.SchemaField("accountProfileImage","STRING"),
                    bigquery.SchemaField("accountSubscriberCount","INTEGER"),
                    bigquery.SchemaField("accountUrl","STRING"),
                    bigquery.SchemaField("accountPlatform","STRING"),
                    bigquery.SchemaField("accountPlatformId","INTEGER"),
                    bigquery.SchemaField("accountAccountType","STRING"),
                    bigquery.SchemaField("accountPageAdminTopCountry","STRING"), #
                    bigquery.SchemaField("accountVerified","STRING"),
                    bigquery.SchemaField("imageText","STRING"), #
                    bigquery.SchemaField("videoLengthMS","FLOAT"), #
                    bigquery.SchemaField("liveVideoStatus","STRING"), #
                    bigquery.SchemaField("newId","STRING"),
                    bigquery.SchemaField("id","STRING")
                ]

output_history_fields=[
                        "historyActualTimestep",
                        "historyActualDate",
                        "historyActualScore",
                        "historyActualLikeCount",
                        "historyActualShareCount",
                        "historyActualCommentCount",
                        "historyActualLoveCount",
                        "historyActualWowCount",
                        "historyActualHahaCount",
                        "historyActualSadCount",
                        "historyActualAngryCount",
                        "historyActualThankfulCount",
                        "historyActualCareCount",
                        "historyExpectedLikeCount",
                        "historyExpectedShareCount",
                        "historyExpectedCommentCount",
                        "historyExpectedLoveCount",
                        "historyExpectedWowCount",
                        "historyExpectedHahaCount",
                        "historyExpectedSadCount",
                        "historyExpectedAngryCount",
                        "historyExpectedThankfulCount",
                        "historyExpectedCareCount"
                    ]

main_data_schema = [
    bigquery.SchemaField("accountAccountType","STRING"),
    bigquery.SchemaField("accountHandle","STRING"),
    bigquery.SchemaField("accountId","INTEGER"),
    bigquery.SchemaField("accountName","STRING"),
    bigquery.SchemaField("accountPageAdminTopCountry","STRING"),
    bigquery.SchemaField("accountPlatform","STRING"),
    bigquery.SchemaField("accountPlatformId","INTEGER"),
    bigquery.SchemaField("accountProfileImage","STRING"),
    bigquery.SchemaField("accountSubscriberCount","INTEGER"),
    bigquery.SchemaField("accountUrl","STRING"),
    bigquery.SchemaField("accountVerified","INTEGER"),
    bigquery.SchemaField("actualAngryCount","INTEGER"),
    bigquery.SchemaField("actualCareCount","INTEGER"),
    bigquery.SchemaField("actualCommentCount","INTEGER"),
    bigquery.SchemaField("actualHahaCount","INTEGER"),
    bigquery.SchemaField("actualLikeCount","INTEGER"),
    bigquery.SchemaField("actualLoveCount","INTEGER"),
    bigquery.SchemaField("actualSadCount","INTEGER"),
    bigquery.SchemaField("actualShareCount","INTEGER"),
    bigquery.SchemaField("actualThankfulCount","INTEGER"),
    bigquery.SchemaField("actualWowCount","INTEGER"),
    bigquery.SchemaField("caption","STRING"),
    bigquery.SchemaField("date","TIMESTAMP"),
    bigquery.SchemaField("description","STRING"),
    bigquery.SchemaField("expectedAngryCount","INTEGER"),
    bigquery.SchemaField("expectedCareCount","INTEGER"),
    bigquery.SchemaField("expectedCommentCount","INTEGER"),
    bigquery.SchemaField("expectedHahaCount","INTEGER"),
    bigquery.SchemaField("expectedLikeCount","INTEGER"),
    bigquery.SchemaField("expectedLoveCount","INTEGER"),
    bigquery.SchemaField("expectedSadCount","INTEGER"),
    bigquery.SchemaField("expectedShareCount","INTEGER"),
    bigquery.SchemaField("expectedThankfulCount","INTEGER"),
    bigquery.SchemaField("expectedWowCount","INTEGER"),
    bigquery.SchemaField("id","STRING"),
    bigquery.SchemaField("imageText","STRING"),
    bigquery.SchemaField("link","STRING"),
    bigquery.SchemaField("liveVideoStatus","STRING"),
    bigquery.SchemaField("message","STRING"),
    bigquery.SchemaField("newId","STRING"),
    bigquery.SchemaField("platform","STRING"),
    bigquery.SchemaField("platformId","STRING"),
    bigquery.SchemaField("postUrl","STRING"),
    bigquery.SchemaField("score","FLOAT"),
    bigquery.SchemaField("subscriberCount","INTEGER"),
    bigquery.SchemaField("title","STRING"),
    bigquery.SchemaField("type","STRING"),
    bigquery.SchemaField("updated","TIMESTAMP"),
    bigquery.SchemaField("videoLengthMS","FLOAT")
]

history_schema = [
    bigquery.SchemaField("historyActualAngryCount","INTEGER"),
    bigquery.SchemaField("historyActualCareCount","INTEGER"),
    bigquery.SchemaField("historyActualCommentCount","INTEGER"),
    bigquery.SchemaField("historyActualDate","TIMESTAMP"),
    bigquery.SchemaField("historyActualHahaCount","INTEGER"),
    bigquery.SchemaField("historyActualLikeCount","INTEGER"),
    bigquery.SchemaField("historyActualLoveCount","INTEGER"),
    bigquery.SchemaField("historyActualSadCount","INTEGER"),
    bigquery.SchemaField("historyActualScore","FLOAT"),
    bigquery.SchemaField("historyActualShareCount","INTEGER"),
    bigquery.SchemaField("historyActualThankfulCount","INTEGER"),
    bigquery.SchemaField("historyActualTimestep","INTEGER"),
    bigquery.SchemaField("historyActualWowCount","INTEGER"),
    bigquery.SchemaField("historyExpectedAngryCount","INTEGER"),
    bigquery.SchemaField("historyExpectedCareCount","INTEGER"),
    bigquery.SchemaField("historyExpectedCommentCount","INTEGER"),
    bigquery.SchemaField("historyExpectedHahaCount","INTEGER"),
    bigquery.SchemaField("historyExpectedLikeCount","INTEGER"),
    bigquery.SchemaField("historyExpectedLoveCount","INTEGER"),
    bigquery.SchemaField("historyExpectedSadCount","INTEGER"),
    bigquery.SchemaField("historyExpectedShareCount","INTEGER"),
    bigquery.SchemaField("historyExpectedThankfulCount","INTEGER"),
    bigquery.SchemaField("historyExpectedWowCount","INTEGER"),
    bigquery.SchemaField("platformId","STRING"),
]

media_schema = [
    bigquery.SchemaField("mediaFull","STRING"),
    bigquery.SchemaField("mediaHeight","FLOAT"),
    bigquery.SchemaField("mediaType","STRING"),
    bigquery.SchemaField("mediaUrl","STRING"),
    bigquery.SchemaField("mediaWidth","FLOAT"),
    bigquery.SchemaField("platformId","STRING"),
]
expanded_links_schema = [
    bigquery.SchemaField("expandedLinksExpanded","STRING"),
    bigquery.SchemaField("expandedLinksOriginal","STRING"),
    bigquery.SchemaField("platformId","STRING")
]

# accountAccountType, 
# accountHandle, 
# accountId, 
# accountName, 
# accountPageAdminTopCountry, 
# accountPlatform, 
# accountPlatformId, 
# accountProfileImage, 
# accountSubscriberCount, 
# accountUrl, 
# accountVerified, 
# actualAngryCount, 
# actualCareCount, 
# actualCommentCount, 
# actualHahaCount, 
# actualLikeCount, 
# actualLoveCount, 
# actualSadCount, 
# actualShareCoount, 
# actualThankfulCount, 
# actualWowcount, 
# caption, 
# date, 
# description, 
# expectedAngryCount, 
# expectedCareCount, 
# expectedCommentCount, 
# expectedHahaCount, 
# expectedLikeCount, 
# expectedLoveCount, 
# expectedSadCount, 
# expectedShareCoount, 
# expectedThankfulCount, 
# expectedWowcount, 
# id, 
# imageText, 
# link, 
# liveVideoStatus, 
# message, 
# newId,
# platform,
# platformId,
# postUrl,
# score,
# subscriberCount,
# title,
# type,
# updated,
# videoLengthMS