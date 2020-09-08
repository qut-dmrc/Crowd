import datetime
import click
import csv
import logging
import yaml
import os
import pandas as pd
import numpy as np
from socialreaper.apis import API
from socialreaper.tools import flatten,fill_gaps
from .crowdapi import CrowdTangle
# from .tools import CSV
from backports.datetime_fromisoformat import MonkeyPatch
MonkeyPatch.patch_fromisoformat()

@click.command()
# @click.option('--endpoint', nargs=1, type=click.Choice(['posts', 'posts/search'], case_sensitive=False), required=True)
@click.option('-config','--config', nargs=1, default="default_query.yml")
@click.option('-t','--token', nargs=1)
@click.option('-ls','--lists', nargs=1)
@click.option('-s','--search_terms', nargs=1)
@click.option('-sdate','--start_date', nargs=1)
@click.option('-edate','--end_date', nargs=1)
@click.option('--output_filename', nargs=1)
@click.option('-off','--offset', nargs=1)
@click.option('-log','--log', 'log', flag_value=True)
@click.option('-links','--links', nargs=1)
def main(config, token, lists, search_terms, start_date, end_date, output_filename, offset, log, links):
    if config:
        with open(os.path.join(os.getcwd(),config)) as f:
            params = yaml.full_load(f)

        # convert params to variables       
        endpoint = params['endpoint']
        if not token:
            token = params['token']
        if not start_date:
            start_date = params['start_date']
        if not end_date:
            end_date = params['end_date'] or datetime.datetime.now().isoformat()
        if not output_filename:
            output_filename = params['output_filename'] or "{}.csv".format(datetime.datetime.now().replace(microsecond=0).isoformat().replace(":",'.'))
        if not offset:
            offset = params['offset'] or 0
        if not log:
            log = params['log'] or False
        # posts/search endpoint
        if endpoint == "posts/search":
            if not lists:
                lists = params['lists']
            if not search_terms:
                search_terms = params['search_terms'] or ""
        if endpoint == "links":
            if not links:
                links = params['links'] or []
        

    if start_date:
        start_date = datetime.datetime.fromisoformat(start_date)
    if end_date:
        end_date = datetime.datetime.fromisoformat(end_date)
    
    # print(start_date, type(start_date))
    # print(end_date, type(end_date))
    # print(token, type(token))
    # print(lists, type(lists))
    # print(search_terms, type(search_terms))
    # print(output_filename, type(output_filename))
    # print(offset, type(offset))
    # print(log, type(log))
    # print(links, type(links))


    timeFrames = getTimeframeList(start_date, end_date)
    if log:
        logging.basicConfig(filename='paging.log',level=logging.INFO)
    if lists:
        inListIds = lists.strip().replace(" ","")
    else:
        inListIds = None
    actualStartDate = end_date
    prevActualStartDate = None
    # result = pd.read_csv('russia-june.csv')
    # prevActualStartDate = None
    # actualStartDate = pd.DataFrame(result).iloc[-1].at['date']
    # actualStartDate = datetime.datetime.fromisoformat(actualStartDate)
    # if actualStartDate>start_date:
    #     end_date = actualStartDate
    # while(not start_date or actualStartDate>start_date and actualStartDate != prevActualStartDate):
    timeFrames = getTimeframeList(start_date, actualStartDate)
    for timeframe in timeFrames:
        start = timeframe[0]
        end = timeframe[1]
        ct = CrowdTangle(token)
        print("Retrieving from {} to {}".format(start,end))
        if endpoint=='posts/search':
            res = ct.postSearch(search_term=search_terms, inListIds=inListIds, start_date=start, end_date=end, offset=offset)
            if res['result'] and res['result']['posts']:
                # if actualstartDate in res['result']:
                #     nextEndDate
                # flatten dictionary and fill gap
                fieldnames, data = fill_gaps([flatten(datum) for datum in list(res['result']['posts'])])
                with open(output_filename, 'a', encoding='utf-8', errors='ignore', newline='') as f:
                # CSV(list(res['result']['posts']), file_name=output_filename, append=True)
                    writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator='\n', extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(data)
                    while 'nextPage' in res['result']['pagination']:
                        nextPage = res['result']['pagination']['nextPage']
                        if log:
                            logging.info(nextPage)
                        # retrieve next page end point and params
                        nextPageParams = nextPage.replace("https://api.crowdtangle.com/", "")
                        res = ct.api_call(nextPageParams,"")
                        
                        fieldnames, data = fill_gaps([flatten(datum) for datum in list(res['result']['posts'])])
                        writer.writerows(data)
                        # CSV(list(res['result']['posts']), file_name=output_filename, append=True)
        elif endpoint=="links":
            for i in range(len(links)):
                res = ct.links(link=links[i], start_date=start, end_date=end, offset=offset)
                if res['result'] and res['result']['posts']:
                    # if actualstartDate in res['result']:
                    #     nextEndDate
                    # flatten dictionary and fill gap
                    fieldnames, data = fill_gaps([flatten(datum) for datum in list(res['result']['posts'])])
                    with open(output_filename, 'a', encoding='utf-8', errors='ignore', newline='') as f:
                    # CSV(list(res['result']['posts']), file_name=output_filename, append=True)
                        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator='\n', extrasaction='ignore')
                        if i == 0:
                            writer.writeheader()
                        writer.writerows(data)
                        while 'nextPage' in res['result']['pagination']:
                            nextPage = res['result']['pagination']['nextPage']
                            if log:
                                logging.info(nextPage)
                            # retrieve next page end point and params
                            nextPageParams = nextPage.replace("https://api.crowdtangle.com/", "")
                            res = ct.api_call(nextPageParams,"")
                            
                            fieldnames, data = fill_gaps([flatten(datum) for datum in list(res['result']['posts'])])
                            writer.writerows(data)
        else:
            print('End point {} not supported'.format(endpoint))
        # # getting actual start date
        # prevActualStartDate = actualStartDate 
        # header = pd.read_csv(output_filename, nrows=1)
        # headers = header.columns.values
        # dateIndex = np.where( headers=='date')
        # with open(output_filename,'r',encoding='utf-8') as result:
        #     last_line = result.readlines()[-1].strip().split(',')
        #     actualStartDate = np.array(last_line)[dateIndex][0]
        #     actualStartDate = datetime.datetime.fromisoformat(actualStartDate)
        # if actualStartDate>start_date:
        #     end_date = actualStartDate

# split timeframe into a list of timeframes with each is of maximum one year margin
def getTimeframeList(startDate, endDate):
    timeList = []
    if not startDate:
        startDate = endDate-datetime.timedelta(days=365)+datetime.timedelta(seconds=1)
    while (endDate - startDate).days >= 365:
        timeFrameStart = endDate-datetime.timedelta(days=365)+datetime.timedelta(seconds=1)
        timeList.append([timeFrameStart.isoformat(), endDate.isoformat()])
        endDate = timeFrameStart-datetime.timedelta(seconds=1)
    timeList.append([startDate.isoformat(), endDate.isoformat()])
    return timeList

if __name__ == '__main__':
    main()