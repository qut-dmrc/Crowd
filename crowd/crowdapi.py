from socialreaper.apis import API

class CrowdTangle(API):
    def __init__(self, api_key):
        super().__init__()

        self.key = api_key
        self.url = "https://api.crowdtangle.com"

    def api_call(self, edge, parameters, return_results=True):
        req = self.get("%s/%s" % (self.url, edge), params=parameters)

        if not req:
            return None

        if return_results:
            return req.json()

    ## The largest margin between startDate and endDate must be less than one year.
    ##TODO timeframe must be sql interval format
    def postSearch(self, search_term, count=100, account_types=None, and_kw=None, not_kw=None, branded_content="no_filter",
                end_date=None, include_history=None, in_account_ids=None, in_list_ids=None, language=None,
                min_interactions=0, min_subscriber_count=0, not_in_account_ids=None, not_in_list_ids=None, not_in_title=None,
                offset=0, page_admin_top_country=None, platforms=None, search_field="text_fields_and_image_text",
                sort_by="date",start_date=None, timeframe=None, types=None, verified="no_filter", verified_only=False, 
               **params):

        count = 100 if count > 100 else count
        parameters = {"searchTerm": search_term,
                      "count": count,
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

    # def guess_channel_id(self, username, count=5):
    #     parameters = {
    #         "forUsername": username,
    #         "part": "id",
    #         "maxResults": count,
    #         "key": self.key
    #     }
    #     return self.api_call('channels', parameters)['items']
