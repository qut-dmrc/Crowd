import datetime
import click
import csv
from socialreaper.apis import API
from socialreaper.tools import flatten,fill_gaps
from .crowdapi import CrowdTangle
# from .tools import CSV
from backports.datetime_fromisoformat import MonkeyPatch
MonkeyPatch.patch_fromisoformat()
import logging

@click.command()
@click.argument('endpoint', nargs=1, type=click.Choice(['posts', 'posts/search'], case_sensitive=False), required=True)
@click.option('-token','--token', nargs=1, required=True)
@click.option('-ls','--lists', nargs=1)
@click.option('-s','--search_terms', nargs=1, default="")
@click.option('-sdate','--start_date', nargs=1, type=datetime.datetime.fromisoformat)
@click.option('-edate','--end_date', nargs=1, type=datetime.datetime.fromisoformat, default=datetime.datetime.now().isoformat())
@click.option('--output_filename', nargs=1, default="{}.csv".format(datetime.datetime.now().replace(microsecond=0).isoformat().replace(":",'.')))
@click.option('-off','--offset', nargs=1, default=0)
@click.option('-log','--log', 'log', flag_value=True, default=False)
def main(token, endpoint, lists, search_terms, start_date, end_date, output_filename, offset, log):
    timeFrames = getTimeframeList(start_date, end_date)
    if log:
        logging.basicConfig(filename='paging.log',level=logging.INFO)
    if lists:
        inListIds = lists.strip().replace(" ","")
    else:
        inListIds = None
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
        else:
            print('only posts/search is supported')

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