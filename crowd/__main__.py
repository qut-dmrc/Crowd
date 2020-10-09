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
from backports.datetime_fromisoformat import MonkeyPatch
MonkeyPatch.patch_fromisoformat()

@click.command()
# @click.option('--endpoint', nargs=1, type=click.Choice(['posts', 'posts/search'], case_sensitive=False), required=True)
@click.option('-config','--config', nargs=1, default="default_query.yml")
@click.option('-t','--token', nargs=1)
@click.option('-ls','--lists', nargs=1)
@click.option('-s','--search_terms', nargs=1)
@click.option('-and', '--and_terms', nargs=1)
@click.option('-not','--not_terms', nargs=1)
@click.option('-sdate','--start_date', nargs=1)
@click.option('-edate','--end_date', nargs=1)
@click.option('--output_filename', nargs=1)
@click.option('-off','--offset', nargs=1)
@click.option('-log','--log', 'log', flag_value=True)
@click.option('-h','--history', 'history', flag_value=True)
@click.option('-links','--links', nargs=1)
@click.option('-ids','--ids', nargs=1)
def main(config, token, lists, search_terms, and_terms, not_terms, start_date, end_date, output_filename, offset, log, history, links, ids):
    if config:
        with open(os.path.join(os.getcwd(),config)) as f:
            params = yaml.full_load(f)

        # convert params to variables       
        endpoint = params['endpoint']
        if not token:
            token = params['token']
        if not output_filename:
            output_filename = params['output_filename'] or "{}.csv".format(datetime.datetime.now().replace(microsecond=0).isoformat().replace(":",'.'))
        if not log:
            log = params['log'] or False

        # posts/search endpoint
        if endpoint == "posts/search":
            if not start_date:
                start_date = params['start_date']
            if not end_date:
                end_date = params['end_date'] or datetime.datetime.now().isoformat()
            if not offset:
                offset = params['offset'] or 0
            if not lists:
                lists = params['lists']
            if not search_terms:
                search_terms = params['search_terms'] or ""
            if not and_terms:
                and_terms = params['AND_terms'] or None
            if not not_terms:
                not_terms = params['NOT_terms'] or None
            if not history:
                history = params['history'] or False
            
        if endpoint == "links":
            if not start_date:
                start_date = params['start_date']
            if not end_date:
                end_date = params['end_date'] or datetime.datetime.now().isoformat()
            if not offset:
                offset = params['offset'] or 0
            if not links:
                links = params['links'] or []
        
        if endpoint == "post":
            if not ids:
                ids = params['ids'] or ""
            if not history:
                history = params['history'] or False
        

    if start_date:
        start_date = datetime.datetime.fromisoformat(start_date)
    if end_date:
        end_date = datetime.datetime.fromisoformat(end_date)
    if log:
        logging.basicConfig(filename='paging.log',level=logging.INFO)
    if lists:
        inListIds = lists.strip().replace(" ","")
    else:
        inListIds = None
    last_date = None
    if endpoint == "post":
        ids = ids.strip().replace(" ","").split(",")
        ct = CrowdTangle(token)
        nodes = []
        for _id in ids:
            print("Retrieving post {}".format(_id))
            res = ct.post(_id, include_history=history)
            if res['result'] and res['result']['posts']:
                result =  res['result']['posts'][0]
                timesteps = result['history']
                del result['history']
                for timestep in timesteps:
                    timestep['Score Date'] = timestep['date']
                    node = {**timestep, **result}
                    nodes.append(node)
        fieldnames, data = fill_gaps([flatten(datum) for datum in nodes])
        with open(output_filename, 'a', encoding='utf-8', errors='ignore', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator='\n', extrasaction='ignore')
            writer.writeheader()
            writer.writerows(data)
    else:
        actualStartDate = end_date
        prevActualStartDate = None
        hasHeader = False
        while(not start_date or actualStartDate>start_date and actualStartDate != prevActualStartDate):
            timeFrames = getTimeframeList(start_date, actualStartDate)
            for timeframe in timeFrames:
                start = timeframe[0]
                end = timeframe[1]
                ct = CrowdTangle(token)
                print("Retrieving from {} to {}".format(start,end))
                if endpoint=='posts/search':
                    res = ct.postSearch(search_term=search_terms, and_kw=and_terms, not_kw=not_terms, inListIds=inListIds, start_date=start, end_date=end, offset=offset, include_history=history)

                    if res['result'] and res['result']['posts']:
                        # flatten dictionary and fill gap
                        fieldnames, data = fill_gaps([flatten(datum) for datum in list(res['result']['posts'])])
                        last_date = data[-1]['date']
                        with open(output_filename, 'a', encoding='utf-8', errors='ignore', newline='') as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator='\n', extrasaction='ignore')
                            # write header once
                            if not hasHeader:
                                writer.writeheader()
                                hasHeader = True
                            writer.writerows(data)
                            # post_ids = ["838988326148968_3047914181923027","2281959232017693_2633403723539907"]
                            # filteredData = [datum for datum in data if datum['platformId'] in post_ids]
                            # writer.writerows(filteredData)
                            # break
                            while 'nextPage' in res['result']['pagination']:
                                nextPage = res['result']['pagination']['nextPage']
                                if log:
                                    logging.info(nextPage)
                                # retrieve next page end point and params
                                nextPageParams = nextPage.replace("https://api.crowdtangle.com/", "")
                                if not search_terms or search_terms == "":
                                    nextPageParams = nextPageParams + "&searchTerm="
                                res = ct.api_call(nextPageParams,"")
                                # print(res)
                                fieldnames, data = fill_gaps([flatten(datum) for datum in list(res['result']['posts'])])
                                last_date = data[-1]['date']
                                writer.writerows(data)
                elif endpoint=="links":
                    for i in range(len(links)):
                        res = ct.links(link=links[i], start_date=start, end_date=end, offset=offset)
                        if res['result'] and res['result']['posts']:
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
            
            # getting actual start date
            prevActualStartDate = actualStartDate
            # header = pd.read_csv(output_filename, nrows=1)
            # headers = header.columns.values
            # dateIndex = np.where( headers=='date')
            # with open(output_filename,'r',encoding='utf-8') as result:
                # last_line = result.readlines()[-1].strip().split(',')
                # actualStartDate = np.array(last_line)[dateIndex][0]
                # actualStartDate = last_line[2]
            actualStartDate = last_date
            # print(actualStartDate)
            actualStartDate = datetime.datetime.fromisoformat(actualStartDate)
            if actualStartDate>start_date:
                end_date = actualStartDate

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