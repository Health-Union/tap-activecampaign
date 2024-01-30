import requests
from tap_activecampaign.streams import Campaign_report_open_list
from tap_activecampaign.client_v1 import ActiveCampaignClientV1
import unittest
from unittest import mock


# mocked response class
class Mockresponse:
    def __init__(self, status_code, json, raise_error, headers=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.headers = headers

    def raise_for_status(self):
        """
        Raise error if 'raise_error' is True
        """
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        """
        Return mocked response
        """
        return self.text


# function to get mocked response
def get_response(status_code, json={}, raise_error=False, headers=None):
    return Mockresponse(status_code, json, raise_error, headers)


class TestActivitiesStreamParams(unittest.TestCase):
    @mock.patch("requests.Session.request")
    @mock.patch("tap_activecampaign.streams.Campaign_report_open_list.process_records")
    def test_activities_stream_params(self, mocked_process_records, mocked_request):
        """
        Test case to verify the param `after` is set as expected for Campaign_report_open_list during API call
        """
        # mock request and return value
        mocked_request.side_effect = [
            get_response(
                200,
                """<?xml version='1.0' encoding='utf-8'?>
                    <not_allowed><error>You are not authorized to access this file</error></not_allowed>
                """
            ),
            get_response(
                200,
                {
                    "0": {
                        "subscriberid": "111",
                        "email": "test03@gmail1.com",
                        "tstamp": "2023-08-28 08:37:19",
                        "times": "1",
                        "tstamp_iso": "2023-08-28T07:37:19-05:00",
                    },
                    "1": {
                        "subscriberid": "222",
                        "email": "test04@gmail.com",
                        "tstamp": "2023-08-29 08:37:19",
                        "times": "1",
                        "tstamp_iso": "2023-08-29T07:37:19-05:00",
                    },
                    "result_code": 1,
                    "result_message": "Success: Something is returned",
                    "result_output": "json",
                },
            ),
            get_response(
                200,
                {
                    "0": {
                        "subscriberid": "333",
                        "email": "test03@gmail1.com",
                        "tstamp": "2023-08-30 08:37:19",
                        "times": "1",
                        "tstamp_iso": "2023-30-28T07:37:19-05:00",
                    },
                    "1": {
                        "subscriberid": "444",
                        "email": "test04@gmail.com",
                        "tstamp": "2023-08-31 08:37:19",
                        "times": "1",
                        "tstamp_iso": "2023-08-31T07:37:19-05:00",
                    },
                    "result_code": 1,
                    "result_message": "Success: Something is returned",
                    "result_output": "json",
                },
            ),
            get_response(
                200,
                {
                    "result_code": 0,
                    "result_message": "Failed: Nothing is returned",
                    "result_output": "json",
                },
            ),
        ]

        # mock 'process_records' and return value
        mocked_process_records.side_effect = [( "2023-08-28", 2 ),  ( "2023-08-28", 2 )]

        # create client
        client = ActiveCampaignClientV1(
            "test_client_id", "test_client_secret", "test_refresh_token"
        )
        # create 'Activities' stream object
        campaign_report_open_list = Campaign_report_open_list(client=client)
        # function call
        total_extracted_entries = campaign_report_open_list.sync(
            client,
            {},
            {},
            "2022-04-01",
            campaign_report_open_list.path,
            ["campaign_report_open_list"],
            campaigns=[123],
        )

        # get arguments passed during calling "requests.Session.request"
        args, kwargs = mocked_request.call_args
        # get 'params' value from passed arguments
        params = kwargs.get("params")

        
        # verify the 'page & campaignid' param are provided
        self.assertTrue("page=3&campaignid=123" in params)

        # verify number of extracted entries:
        self.assertEqual( total_extracted_entries, 4, "The number of extracted entries doesn't match the expected count")