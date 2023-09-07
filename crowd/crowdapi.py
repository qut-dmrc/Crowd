from os import environ
from time import sleep
import requests
import logging
import datetime
import yaml
import csv
import concurrent.futures
from PIL import Image
from io import BytesIO

from .exceptions import *
from .togbq import *


class API:
    def __init__(self, rate_limit=None, session=None):
        self.log_function = print
        self.retry_rate = 10  #
        self.num_retries = 4  # default
        self.failed_last = False
        self.force_stop = False
        self.ignore_errors = False
        self.common_errors = (requests.exceptions.ConnectionError,
                              requests.exceptions.Timeout,
                              requests.exceptions.HTTPError)

        self.rate_limit = rate_limit
        self.remaining_req = rate_limit
        self.session = session

    def __str__(self):
        return pformat(vars(self))

    def log_error(self, e):
        """
        Print errors. Stop travis-ci from leaking api keys

        :param e: The error
        :return: None
        """

        if not environ.get('CI'):
            self.log_function(e)
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                self.log_function(e.response.text)
                logging.warning(e.response.text)

    def _sleep(self, seconds):
        """
        Sleep between requests, but don't force asynchronous code to wait

        :param seconds: The number of seconds to sleep
        :return: None
        """
        for _ in range(int(seconds)):
            if not self.force_stop:
                sleep(1)

    @staticmethod
    def merge_params(parameters, new):
        if new:
            parameters = {**parameters, **new}
        return parameters

    def get(self, *args, **kwargs):

        """
        An interface for get requests that handles errors more gracefully to
        prevent data loss
        """
        # Safeguard the rate limit, run request only when there is quota left
        if self.remaining_req <= 0:
            sleep_time = 60 - datetime.datetime.now().second
            self.log_function("New request available in %s seconds" % sleep_time)
            self._sleep(sleep_time)  # wait until the start of next minute
            self.remaining_req = self.rate_limit
        try:
            req_func = self.session.get if self.session else requests.get
            req = req_func(*args, **kwargs)
            logging.info(
                "%s:Retrieving from url: %s" % (datetime.datetime.now(), req.url))
            self.log_function("Url: %s" % req.url)
            req.raise_for_status()
            self.failed_last = False
            self.remaining_req -= 1
            return req

        except requests.exceptions.RequestException as e:
            self.log_error(e)
            for i in range(1, self.num_retries):
                sleep_time = self.retry_rate * i
                self.log_function("Retrying in %s seconds" % sleep_time)
                self._sleep(sleep_time)
                try:
                    req = requests.get(*args, **kwargs)
                    logging.info("%s:Retrieving from url: %s" % (
                    datetime.datetime.now(), req.url))
                    self.log_function("Url: %s" % req.url)
                    req.raise_for_status()
                    self.remaining_req -= 1
                    self.log_function("New request successful")
                    return req
                except requests.exceptions.RequestException:
                    self.log_function("New request failed")
                    logging.warning("New request failed")

            # Allows for the api to ignore one potentially bad request
            if not self.failed_last:
                self.failed_last = True
                return None;
                # raise ApiError(e)
            else:
                raise FatalApiError(e)


class CrowdTangle(API):
    '''
    The rate limit of CT refreshes at the beginnning of every minute, not on 60-second sliding window.
    E.g. For rate limit of 2 requests/minute, When you request successfully at 16:11:56 and 16:11:59, 
    you are able to make another two requests at 16:12:00, 16:12:05, despite them being in the same 60-second window
    '''
    def __init__(self, config, append=False):
        self.url = "https://api.crowdtangle.com"
        self.read_config(config)
        # 40 columns when includeHistory = False
        self.fieldnames = [ "platformId", 
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
                            "actualCommentCount",
                            "expectedLikeCount",
                            "expectedCommentCount",
                            "accountId",
                            "accountName",
                            "accountHandle",
                            "accountProfileImage",
                            "accountSubscriberCount",
                            "accountUrl",
                            "accountPlatform",
                            "accountPlatformId",
                            "accountVerified",
                            "imageText", #
                            "videoLengthMS", #
                            "liveVideoStatus", #
                            "newId",
                            "id"]
        if self.history:
            # 47 columns when includeHistory = True

            self.fieldnames = self.fieldnames + \
                              [
                                  "historyActualTimestep",
                                  "historyActualDate",
                                  "historyActualScore",
                                  "historyActualLikeCount",
                                  "historyActualCommentCount",
                                  "historyExpectedLikeCount",
                                  "historyExpectedCommentCount"
                              ]
        if not append:
            # write header once
            with open(self.output_filename, 'w', encoding='utf-8', errors='ignore',
                    newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.fieldnames)
            self.earliestStartDate = None
        super().__init__(self.rate_limit)

    def read_config(self, config, rate_limit=6):
        logging.basicConfig(filename='info_insta.log', level=logging.INFO)
        with open(os.path.join(os.getcwd(), config)) as f:
            params = yaml.full_load(f)
            # convert params to variables       
            self.endpoint = params['endpoint']
            self.key = params['token']
            self.log = params['log'] or False
            self.output_filename = params['output_filename'] or \
                                   "{}.csv".format(datetime.datetime.now().replace(
                                       microsecond=0).isoformat().replace(":", '.'))
            self.rate_limit = rate_limit
            self.downloadImages = params['downloadImages']
            self.history = params['history'] or False
            self.togbq = params['togbq'] or False
            if self.togbq:
                self.bq_credential = params['bq_credential']
                self.bq_table_id = params['bq_table_id']

            # posts/search endpoint
            if self.endpoint == "posts/search" or self.endpoint == "posts":
                self.lists = params['lists']
                self.accounts = params['accounts']
                self.search_terms = params['search_terms'] or ""
                self.and_terms = params['AND_terms'] or None
                self.not_terms = params['NOT_terms'] or None
                self.accounts = params['accounts'] or None
                self.offset = params['offset'] or 0
                self.start_date = params['start_date']
                self.end_date = params[
                                    'end_date'] or datetime.datetime.now().isoformat()
                self.inListIds = self.lists.strip().replace(" ",
                                                            "") if self.lists else None

            if self.endpoint == "links":
                self.links = params['links'] or []
                self.rate_limit = 2
                self.offset = params['offset'] or 0
                self.start_date = params['start_date']
                self.end_date = params[
                                    'end_date'] or datetime.datetime.now().isoformat()

            if self.endpoint == "post":
                self.ids = params['ids'] or ""
                self.history = params['history'] or False
                self.ids = self.ids.strip().replace(" ", "").split(",")
                self.start_date= None # post endpoint is included in runTimeFrame which requires start_date and end_date
                self.end_date= None # post endpoint is included in runTimeFrame which requires start_date and end_date

        # clean data
        if self.endpoint != "post":
            self.end_date = datetime.datetime.fromisoformat(self.end_date)
            self.start_date = datetime.datetime.fromisoformat(
                self.start_date) if self.start_date else \
                self.end_date - datetime.timedelta(days=365) + datetime.timedelta(
                    seconds=1)

    def api_call(self, edge, parameters, return_results=True):
        # if self.remaining_req <= 0:
        #     sleep_time = 60-datetime.datetime.now().second
        #     self.log_function("Request available in %s seconds" % sleep_time)
        #     self._sleep(sleep_time) # wait until the start of next minute
        #     self.remaining_req = self.rate_limit
        #     print("Limit refreshed, limit now: %s" % self.remaining_req)

        req = self.get("%s/%s" % (self.url, edge), params=parameters)

        if not req:
            return None

        if return_results:
            return req.json()

    # The largest margin between startDate and endDate must be less than one year.
    # end_date and start_date are string in iso format
    # TODO timeframe must be sql interval format
    def postSearch(self, search_term, count=100, accounts=None, account_types=None,
                    and_kw=None, not_kw=None, branded_content="no_filter",
                   end_date=None, include_history=None, in_account_ids=None,
                   in_list_ids=None, language=None,
                   min_interactions=0, min_subscriber_count=0, not_in_account_ids=None,
                   not_in_list_ids=None, not_in_title=None,
                   offset=0, page_admin_top_country=None, platforms="instagram",
                   search_field="text_fields_and_image_text",
                   sort_by="date", start_date=None, timeframe=None, types=None,
                   verified="no_filter", verified_only=False,
                   **params):

        count = 100 if count > 100 else count
        parameters = {"searchTerm": search_term,
                      "count": count,
                      "accounts": accounts,
                      "accountTypes": account_types,
                      "and": and_kw,
                      "not": not_kw,
                      "brandedContent": branded_content,
                      "endDate": end_date,
                      "includeHistory": include_history,
                      "inAccountIds": in_account_ids,
                      "inListIds": in_list_ids,
                      "language": language,
                      "minInteractions": min_interactions,
                      "minSubscriberCount": min_subscriber_count,
                      "notInAccountIds": not_in_account_ids,
                      "notInListIds": not_in_list_ids,
                      "notInTitle": not_in_title,
                      "offset": offset,
                      "pageAdminTopCountry": page_admin_top_country,
                      "platforms": platforms,
                      "searchField": search_field,
                      "sortBy": sort_by,
                      "startDate": start_date,
                      "timeframe": timeframe,
                      "types": types,
                      "verified": verified,
                      "verifiedOnly": verified_only,
                      "token": self.key}

        parameters = self.merge_params(parameters, params)

        return self.api_call("posts/search", parameters)

        # The largest margin between startDate and endDate must be less than one year.
    # end_date and start_date are string in iso format
    # TODO timeframe must be sql interval format
    def posts(self, search_term, count=100, accounts=None, account_types=None,
                    and_kw=None, not_kw=None, branded_content="no_filter",
                   end_date=None, include_history=None, in_account_ids=None,
                   in_list_ids=None, language=None,
                   min_interactions=0, min_subscriber_count=0, not_in_account_ids=None,
                   not_in_list_ids=None, not_in_title=None,
                   offset=0, page_admin_top_country=None, platforms="instagram",
                   search_field="text_fields_and_image_text",
                   sort_by="date", start_date=None, timeframe=None, types=None,
                   verified="no_filter", verified_only=False,
                   **params):

        count = 100 if count > 100 else count
        parameters = {"searchTerm": search_term,
                      "count": count,
                      "accounts": accounts,
                      "accountTypes": account_types,
                      "and": and_kw,
                      "not": not_kw,
                      "brandedContent": branded_content,
                      "endDate": end_date,
                      "includeHistory": include_history,
                      "inAccountIds": in_account_ids,
                    #   "inListIds": in_list_ids,
                      "listIds": in_list_ids,
                      "language": language,
                      "minInteractions": min_interactions,
                    #   "minSubscriberCount": min_subscriber_count,
                      "notInAccountIds": not_in_account_ids,
                      "notInListIds": not_in_list_ids,
                      "notInTitle": not_in_title,
                      "offset": offset,
                      "pageAdminTopCountry": page_admin_top_country,
                    #   "platforms": platforms,
                      "searchField": search_field,
                      "sortBy": sort_by,
                      "startDate": start_date,
                      "timeframe": timeframe,
                      "types": types,
                      "verified": verified,
                    #   "verifiedOnly": verified_only,
                      "token": self.key}

        parameters = self.merge_params(parameters, params)

        return self.api_call("posts", parameters)

    # The largest margin between startDate and endDate must be less than one year.
    # end_date and start_date are string in iso format
    # TODO timeframe must be sql interval format
    def linksEndpoint(self, link, count=1000, include_history=None, include_summary=None,
                      end_date=None, offset=0, platforms="facebook", search_field=None,
                      sort_by="date", start_date=None, **params):

        count = 1000 if count > 1000 else count
        parameters = {"link": link,
                      "count": count,
                      "endDate": end_date,
                      "includeHistory": include_history,
                      "offset": offset,
                      "platforms": platforms,
                      "searchField": search_field,
                      "sortBy": sort_by,
                      "startDate": start_date,
                      "token": self.key}

        parameters = self.merge_params(parameters, params)

        return self.api_call("links", parameters)

    def post(self, _id, include_history=None, **params):

        parameters = {"includeHistory": include_history,
                      "token": self.key}

        parameters = self.merge_params(parameters, params)

        return self.api_call("post/{}".format(_id), parameters)

    @staticmethod
    def getTimeframeList(startDate, endDate):
        """
        split timeframe into a list of timeframes with each is of maximum one year margin
        startDate: type: datetime
        endDate: type: datetime
        return: timelist - list of list of string
        """
        timeList = []
        while (endDate - startDate).days >= 365:
            timeFrameStart = endDate - datetime.timedelta(
                days=365) + datetime.timedelta(seconds=1)
            timeList.append([timeFrameStart.isoformat(), endDate.isoformat()])
            endDate = timeFrameStart - datetime.timedelta(seconds=1)
        timeList.append([startDate.isoformat(), endDate.isoformat()])
        return timeList

    def flatten(self, post):
        """
        Turn a nested dictionary into a flattened list

        :param post: a post corresponds to a row in the csv 
        :return: A flattened dictionary in list
        """
        row = []
        row.append(post['platformId']) if 'platformId' in post else row.append("")
        row.append(post['platform']) if 'platform' in post else row.append("")
        row.append(post['date']) if 'date' in post else row.append("")
        row.append(post['updated']) if 'updated' in post else row.append("")
        row.append(post['type']) if 'type' in post else row.append("")
        row.append(post['title']) if 'title' in post else row.append("")
        row.append(post['caption']) if 'caption' in post else row.append("")
        row.append(post['description']) if 'description' in post else row.append("")
        row.append(post['message']) if 'message' in post else row.append("")
        if 'expandedLinks' in post and isinstance(post['expandedLinks'], list):
            expandedLinksOriginal = [
                expanded['original'] if 'original' in expanded else "" for expanded in
                post['expandedLinks']]
            expandedLinksExpanded = [
                expanded['expanded'] if 'expanded' in expanded else "" for expanded in
                post['expandedLinks']]
        else:
            expandedLinksOriginal = ""
            expandedLinksExpanded = ""
        row.append(expandedLinksOriginal)
        row.append(expandedLinksExpanded)
        row.append(post['link']) if 'link' in post else row.append("")
        row.append(post['postUrl']) if 'postUrl' in post else row.append("")
        row.append(
            post['subscriberCount']) if 'subscriberCount' in post else row.append("")
        row.append(post['score']) if 'score' in post else row.append("")
        if 'media' in post and isinstance(post['media'], list):
            mediaType = [media['type'] if 'type' in media else "" for media in
                         post['media']]
            mediaUrl = [media['url'] if 'url' in media else "" for media in
                        post['media']]
            if self.downloadImages:
                self.downloadImageFromURL(mediaUrl,self.output_filename.split(".")[0])
            mediaHeight = [media['height'] if 'height' in media else "" for media in
                           post['media']]
            mediaWidth = [media['width'] if 'width' in media else "" for media in
                          post['media']]
            mediaFull = [media['full'] if 'full' in media else "" for media in
                         post['media']]
        else:
            mediaType = ""
            mediaUrl = ""
            mediaHeight = ""
            mediaWidth = ""
            mediaFull = ""
        row.append(mediaType)
        row.append(mediaUrl)
        row.append(mediaHeight)
        row.append(mediaWidth)
        row.append(mediaFull)
        row.append(post['statistics']['actual']['favoriteCount']) if 'favoriteCount' in \
                                                                 post['statistics'][
                                                                     'actual'] else row.append(
            "")
        row.append(post['statistics']['actual']['commentCount']) if 'commentCount' in \
                                                                    post['statistics'][
                                                                        'actual'] else row.append(
            "")
        row.append(post['statistics']['expected']['favouriteCount']) if 'favouriteCount' in \
                                                                   post['statistics'][
                                                                       'expected'] else row.append(
            "")
        row.append(post['statistics']['expected']['commentCount']) if 'commentCount' in \
                                                                      post[
                                                                          'statistics'][
                                                                          'expected'] else row.append(
            "")
        row.append(post['account']['id']) if 'account' in post and 'id' in post[
            'account'] else row.append("")
        row.append(post['account']['name']) if 'account' in post and 'name' in post[
            'account'] else row.append("")
        row.append(post['account']['handle']) if 'account' in post and 'handle' in post[
            'account'] else row.append("")
        row.append(
            post['account']['profileImage']) if 'account' in post and 'profileImage' in \
                                                post['account'] else row.append("")
        row.append(post['account'][
                       'subscriberCount']) if 'account' in post and 'subscriberCount' in \
                                              post['account'] else row.append("")
        row.append(post['account']['url']) if 'account' in post and 'url' in post[
            'account'] else row.append("")
        row.append(post['account']['platform']) if 'account' in post and 'platform' in \
                                                   post['account'] else row.append("")
        row.append(
            post['account']['platformId']) if 'account' in post and 'platformId' in \
                                              post['account'] else row.append("")
        row.append(post['account']['verified']) if 'account' in post and 'verified' in \
                                                   post['account'] else row.append("")
        row.append(post['imageText']) if 'imageText' in post else row.append("")
        row.append(post['videoLengthMS']) if 'videoLengthMS' in post else row.append("")
        row.append(
            post['liveVideoStatus']) if 'liveVideoStatus' in post else row.append("")
        row.append(post['newId']) if 'newId' in post else row.append("")
        row.append(post['id']) if 'id' in post else row.append("")
        if self.history:
            if 'history' in post and isinstance(post['history'],list):
                timestep = [timestamp['timestep'] if 'timestep' in timestamp else "" for timestamp in post['history']]
                date = [timestamp['date'] if 'date' in timestamp else "" for timestamp in post['history']]
                score = [timestamp['score'] if 'score' in timestamp else "" for timestamp in post['history']]
                historyActualLikeCount = [timestamp['actual']['favoriteCount'] if 'favoriteCount' in timestamp['actual'] else "" for timestamp in post['history']]
                historyActualCommentCount = [timestamp['actual']['commentCount'] if 'commentCount' in timestamp['actual'] else "" for timestamp in post['history']]
                historyExpectedLikeCount = [timestamp['expected']['favoriteCount'] if 'favoriteCount' in timestamp['expected'] else "" for timestamp in post['history']]
                historyExpectedCommentCount = [timestamp['expected']['commentCount'] if 'commentCount' in timestamp['expected'] else "" for timestamp in post['history']]
            else:
                timestep = ""
                date = ""
                score = ""
                historyActualLikeCount = ""
                historyActualCommentCount = ""
                historyExpectedLikeCount = ""
                historyExpectedCommentCount = ""
            row.append(timestep)
            row.append(date)
            row.append(score)
            row.append(historyActualLikeCount)
            row.append(historyActualCommentCount)
            row.append(historyExpectedLikeCount)
            row.append(historyExpectedCommentCount)
        return row

    def run(self):
        self.prevStartDate = None
        if self.endpoint == "links":
            for i in range(len(self.links)):
                self.link_end_date = self.end_date
                self.runTimeframes(self.start_date, self.link_end_date, self.links[i])
        else:
            self.runTimeframes(self.start_date, self.end_date)
        append_to_bq(self.bq_credential, self.bq_table_id, self.output_filename) if self.togbq else None
 
    def runTimeframes(self, start_date, end_date, link=None):
        if self.endpoint == "posts/search" or self.endpoint == "posts" or self.endpoint == "links":
            timeFrames = self.getTimeframeList(start_date, end_date)
            for timeframe in timeFrames:
                start = timeframe[0]
                end = timeframe[1]
                self.log_function("Retrieving from {} to {}".format(start, end))
                if self.endpoint == "posts/search":
                    ## break huge account ids into chucks
                    # self.accounts = self.accounts.replace("\n","").replace(" ","").strip().split(',')
                    # chucksize = 100
                    # loop = int(len(self.accounts)/chucksize)+1
                    # for i in range(loop):
                        # print(",".join(self.accounts[i*chucksize:min((i+1)*chucksize,len(self.accounts))]))
                        # self.accountIds = ",".join(self.accounts[i*chucksize:min((i+1)*chucksize,len(self.accounts))])
                    self.accountIds = self.accounts
                    res = self.postSearch(search_term=self.search_terms,
                                        and_kw=self.and_terms, \
                                        not_kw=self.not_terms, in_list_ids=self.inListIds,
                                        accounts=self.accountIds,
                                        start_date=start, \
                                        end_date=end, offset=self.offset,
                                        include_history=self.history)
                    self.processResponse(res)
                if self.endpoint == "posts":
                    self.accountIds = self.accounts
                    res = self.posts(search_term=self.search_terms,
                                        and_kw=self.and_terms, \
                                        not_kw=self.not_terms, in_list_ids=self.inListIds,
                                        accounts=self.accountIds,
                                        start_date=start, \
                                        end_date=end, offset=self.offset,
                                        include_history=self.history)
                    self.processResponse(res)
                if self.endpoint == "links":
                    res = self.linksEndpoint(link,include_history=self.history,\
                                                end_date=end, start_date=start, \
                                                offset= self.offset)
                    self.processResponse(res)
                start = datetime.datetime.fromisoformat(start)
                if self.earliestStartDate and self.earliestStartDate > start and self.earliestStartDate != self.prevStartDate:
                    print("Check timeframe coverage")
                    self.prevStartDate = self.earliestStartDate
                    end = self.earliestStartDate
                    self.offset = 0
                    self.runTimeframes(start, end, link)
        elif self.endpoint == "post":
            self.processResponse()
        else:
            pass

    def processResponse(self, res=None):
        if self.endpoint == "post":
            nodes = []
            for _id in self.ids:
                self.log_function("Retrieving post {}".format(_id))
                res = self.post(_id, include_history=self.history)
                if res['result'] and res['result']['posts']:
                    result = res['result']['posts'][0]
                    nodes.append(self.flatten(result))
            self.writeDataToCSV(nodes)
        elif not res:
            return None
        else:
            if res['result'] and res['result']['posts'] and len(res['result']['posts']) > 0:
                data = [self.flatten(datum) for datum in res['result']['posts']]
                self.writeDataToCSV(data)
                while 'nextPage' in res['result']['pagination']:
                    nextPage = res['result']['pagination']['nextPage']
                    # nextpage without searchTerm will throw API error
                    if self.endpoint == "posts/search" or self.endpoint == "posts":
                        nextPage = nextPage + "&searchTerm=" if not self.search_terms or self.search_terms == "" else nextPage
                        nextPage = nextPage + "&accounts=" + self.accountIds if self.accounts else nextPage
                    res = self.get(nextPage,"")
                    if res:
                        res = res.json()
                        if len(res['result']['posts']) <= 0: return None
                        data = [self.flatten(datum) for datum in res['result']['posts']]
                        self.writeDataToCSV(data)
                    else:
                        return None
            else:
                return None
    
    def writeDataToCSV(self,data):
        with open(self.output_filename, 'a', encoding='utf-8', errors='ignore', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(data)
            self.earliestStartDate = datetime.datetime.fromisoformat(data[-1][2])  # 2 is the position of date

    '''
    Images is a nunpy array of urls of which the images are stored
    '''
    def downloadImageFromURL(self, images, dest_folder):
        if not os.path.isdir(dest_folder): 
            os.mkdir(dest_folder) # create folder
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for url in images:
                executor.submit(self.load_url, url, dest_folder)

    def load_url(self, url, img_folder):
        response = requests.get(url)
        filename = url.split('/')[-1].split("?")[0]
        if response:
            img = Image.open(BytesIO(response.content))
            img.save(os.path.join(img_folder,filename))

