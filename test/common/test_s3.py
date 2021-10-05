"""
Test s3 bucket connector
"""
import os
import unittest
import boto3
from moto import mock_s3
from xetra.common.s3 import S3BucketConnector


class TestS3BucketConnectorMethods(unittest.TestCase):
    def setUp(self):
        """
        setting up environment
        """
        # mocking s3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining class arguements
        self.s3_access_key = "AWS_ACCESS_KEY_ID"
        self.s3_secret_key = "AWS_SECRET_ACCESS_KEY"
        self.s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
        self.s3_bucket_name = "test_bucket"
        # creating se
        os.environ[self.s3_access_key] = "KEY1"
        os.environ[self.s3_secret_key] = "KEY2"
        # creating a bucket environment on mocketd s3
        self.s3 = boto3.resource(service_name="s3", endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(
            Bucket=self.s3_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # creating a testing instance
        self.s3_bucket_conn = S3BucketConnector(
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_endpoint_url,
            self.s3_bucket_name,
        )

    def tearDown(self):
        """ """
        self.mock_s3.stop()

    def test_list_files_prefix_ok(self):
        """
        Test the list_file_in_prefix method for getting 2 file keysas list on the mocked s3 bucket
        """
        # Expected results
        prefix_expec = "prefix/"
        key1_expec = f"{prefix_expec}test1.csv"
        key2_expec = f"{prefix_expec}test2.csv"
        # Test init
        csv_content = """col1, col2
        valA, valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_expec)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_expec)
        # method excution
        list_results = self.s3_bucket_conn.list_files_in_prefix(prefix_expec)
        # Test after method excution
        self.assertEqual(len(list_results), 2)
        self.assertIn(key1_expec, list_results)
        self.assertIn(key2_expec, list_results)
        # clean up after test
        self.s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": key1_expec}, {"Key": key2_expec}]}
        )

    def test_list_files_prefix_wrong(self):
        """
        Test the list_file_in_prefix method for not existing prefix
        """
        # Expected results
        prefix_expec = "no-prefix/"

        # method excution
        list_results = self.s3_bucket_conn.list_files_in_prefix(prefix_expec)
        # Test after method excution
        self.assertTrue(not list_results)


if __name__ == "__main__":
    # unittest.main()
    testIns = TestS3BucketConnectorMethods()
    print("0000")
    testIns.setUp()
    print("1111")
    testIns.test_list_files_prefix_ok()
    testIns.tearDown()
