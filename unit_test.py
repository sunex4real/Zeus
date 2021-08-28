import unittest
import solution
import pandas as pd


class TestZeus(unittest.TestCase):
    """
    Testing the Solution
    """

    def testFalseAddressChangeAndTxid(self):
        """
        Test when address wasn't changed and transaction
        was generated on the frontend
        """

        sample = {
            0: [
                {
                    "hitNumber": 1,
                    "time": 157697,
                    "hour": 17,
                    "isInteraction": True,
                    "isEntrance": None,
                    "isExit": None,
                    "type": "EVENT",
                    "name": None,
                    "landingScreenName": "AllowLocationScreen",
                    "screenName": None,
                    "eventCategory": "ios.product_details",
                    "eventAction": "add_cart.clicked",
                    "eventLabel": "205190",
                    "transactionId": None,
                    "customDimensions": [
                        {"index": 18, "value": "-73.5581377"},
                        {"index": 16, "value": "Montreal"},
                        {"index": 19, "value": "45.5046338"},
                        {"index": 15, "value": "CA"},
                    ],
                },
                {
                    "hitNumber": 2,
                    "time": 548105,
                    "hour": 17,
                    "isInteraction": True,
                    "isEntrance": None,
                    "isExit": None,
                    "type": "EVENT",
                    "name": None,
                    "landingScreenName": "AllowLocationScreen",
                    "screenName": None,
                    "eventCategory": "ios.company_content",
                    "eventAction": "contact_option.clicked",
                    "eventLabel": "chat",
                    "transactionId": None,
                    "customDimensions": [
                        {"index": 36, "value": "s4wy-g8mj"},
                        {"index": 18, "value": "-73.5581377"},
                        {"index": 16, "value": "Montreal"},
                        {"index": 25, "value": "adyen"},
                        {"index": 19, "value": "45.5046338"},
                        {"index": 15, "value": "CA"},
                        {"index": 11, "value": "company_content"},
                    ],
                },
            ]
        }
        sample_data = pd.Series(sample)
        result = solution.get_session_details(sample_data)
        self.assertEqual(result, (False, "s4wy-g8mj"))

    def testTrueAddressChangeAndTxid(self):
        """
        Test when address was changed and transaction
        was generated on the frontend
        """

        sample = {
            0: [
                {
                    "hitNumber": 1,
                    "time": 79395,
                    "hour": 17,
                    "isInteraction": True,
                    "isEntrance": None,
                    "isExit": None,
                    "type": "EVENT",
                    "name": None,
                    "landingScreenName": "AllowLocationScreen",
                    "screenName": None,
                    "eventCategory": "ios.shop_list",
                    "eventAction": "shop.clicked",
                    "eventLabel": "2039",
                    "transactionId": None,
                    "customDimensions": [
                        {"index": 18, "value": "-73.5581377"},
                        {"index": 16, "value": "Montreal"},
                        {"index": 19, "value": "45.5046338"},
                        {"index": 15, "value": "CA"},
                    ],
                },
                {
                    "hitNumber": 2,
                    "time": 548105,
                    "hour": 17,
                    "isInteraction": True,
                    "isEntrance": None,
                    "isExit": None,
                    "type": "EVENT",
                    "name": None,
                    "landingScreenName": "AllowLocationScreen",
                    "screenName": None,
                    "eventCategory": "ios.company_content",
                    "eventAction": "contact_option.clicked",
                    "eventLabel": "chat",
                    "transactionId": None,
                    "customDimensions": [
                        {"index": 36, "value": "s4wy-g8mj"},
                        {"index": 18, "value": "-73.55813599"},
                        {"index": 16, "value": "Montreal"},
                        {"index": 25, "value": "adyen"},
                        {"index": 19, "value": "45.50463486"},
                        {"index": 15, "value": "CA"},
                        {"index": 11, "value": "company_content"},
                    ],
                },
            ]
        }
        sample_data = pd.Series(sample)
        result = solution.get_session_details(sample_data)
        self.assertEqual(result, (True, "s4wy-g8mj"))

    def testTrueAddressChangeAndEmptyTxid(self):
        """
        Test when address was changed and transaction
        wasn't generated on the frontend
        """

        sample = {
            0: [
                {
                    "hitNumber": 1,
                    "time": 79395,
                    "hour": 17,
                    "isInteraction": True,
                    "isEntrance": None,
                    "isExit": None,
                    "type": "EVENT",
                    "name": None,
                    "landingScreenName": "AllowLocationScreen",
                    "screenName": None,
                    "eventCategory": "ios.shop_list",
                    "eventAction": "shop.clicked",
                    "eventLabel": "2039",
                    "transactionId": None,
                    "customDimensions": [
                        {"index": 18, "value": "-73.5581377"},
                        {"index": 16, "value": "Montreal"},
                        {"index": 19, "value": "45.5046338"},
                        {"index": 15, "value": "CA"},
                    ],
                },
                {
                    "hitNumber": 2,
                    "time": 548105,
                    "hour": 17,
                    "isInteraction": True,
                    "isEntrance": None,
                    "isExit": None,
                    "type": "EVENT",
                    "name": None,
                    "landingScreenName": "AllowLocationScreen",
                    "screenName": None,
                    "eventCategory": "ios.company_content",
                    "eventAction": "contact_option.clicked",
                    "eventLabel": "chat",
                    "transactionId": None,
                    "customDimensions": [
                        {"index": 18, "value": "-73.55813599"},
                        {"index": 16, "value": "Montreal"},
                        {"index": 25, "value": "adyen"},
                        {"index": 19, "value": "45.50463486"},
                        {"index": 15, "value": "CA"},
                        {"index": 11, "value": "company_content"},
                    ],
                },
            ]
        }
        sample_data = pd.Series(sample)
        result = solution.get_session_details(sample_data)
        self.assertEqual(result, (True, None))

    def testFalseAddressChangeAndEmptyTxid(self):
        """
        Test when address wasn't changed and transaction
        wasn't generated on the frontend
        """

        sample = {
            0: [
                {
                    "hitNumber": 1,
                    "time": 157697,
                    "hour": 17,
                    "isInteraction": True,
                    "isEntrance": None,
                    "isExit": None,
                    "type": "EVENT",
                    "name": None,
                    "landingScreenName": "AllowLocationScreen",
                    "screenName": None,
                    "eventCategory": "ios.product_details",
                    "eventAction": "add_cart.clicked",
                    "eventLabel": "205190",
                    "transactionId": None,
                    "customDimensions": [
                        {"index": 18, "value": "-73.5581377"},
                        {"index": 16, "value": "Montreal"},
                        {"index": 19, "value": "45.5046338"},
                        {"index": 15, "value": "CA"},
                    ],
                },
                {
                    "hitNumber": 2,
                    "time": 548105,
                    "hour": 17,
                    "isInteraction": True,
                    "isEntrance": None,
                    "isExit": None,
                    "type": "EVENT",
                    "name": None,
                    "landingScreenName": "AllowLocationScreen",
                    "screenName": None,
                    "eventCategory": "ios.company_content",
                    "eventAction": "contact_option.clicked",
                    "eventLabel": "chat",
                    "transactionId": None,
                    "customDimensions": [
                        {"index": 18, "value": "-73.5581377"},
                        {"index": 16, "value": "Montreal"},
                        {"index": 25, "value": "adyen"},
                        {"index": 19, "value": "45.5046338"},
                        {"index": 15, "value": "CA"},
                        {"index": 11, "value": "company_content"},
                    ],
                },
            ]
        }
        sample_data = pd.Series(sample)
        result = solution.get_session_details(sample_data)
        self.assertEqual(result, (False, None))

    def testEmptyTxid(self):
        """
        Test case when transactionid is empty
        """
        txid = None
        result = solution.get_transaction_details(txid)
        self.assertEqual(result, (False, False))

    def testInvalidTxid(self):
        """
        Test case when transactionid is invalid
        i.e not in dataset
        """
        txid = "Zeus"
        result = solution.get_transaction_details(txid)
        self.assertEqual(result, (False, False))

    def testValidTxid(self):
        """
        Test case when transactionid is valid
        i.e in dataset
        """
        txid = "s4wy-g8mj"
        result = solution.get_transaction_details(txid)
        self.assertEqual(result, (True, True))

    def testEmptyVisitorid(self):
        """
        Test case when visitorid empty
        """
        visitorid = None
        result = solution.main(visitorid)
        self.assertEqual(result, {})

    def testInvalidVisitorid(self):
        """
        Test case when visitorid is invalid
        i.e not in GA dataset
        """
        visitorid = "Zeus"
        result = solution.main(visitorid)
        self.assertEqual(result, {})

    def testValidVisitorid(self):
        """
        Test case when visitorid is valid
        i.e in GA dataset
        """
        visitorid = "1768913390307191238"
        response = {
            "full_visitor_id": "1768913390307191238",
            "address_changed": True,
            "is_order_placed": True,
            "Is_order_delivered": True,
            "application_type": "iOS",
        }
        result = solution.main(visitorid)
        self.assertEqual(result, response)


if __name__ == "__main__":
    unittest.main()
