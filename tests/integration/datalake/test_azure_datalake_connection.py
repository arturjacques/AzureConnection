from tests.unit.unit_base_teste import UnitBaseTest
from connectionazure.datalake import ConnectionAzureDataLake

from random import randint

class ConnectionAzureDataLakeTest(UnitBaseTest):       
    def setUp(self) -> None:
        super().setUp()

        self.datalake_connection = ConnectionAzureDataLake()
        self.datalake_connection.initialize_storage_account_ad_env_variable()
        self.test_container = 'test-validation-container'
    def test_if_uploaded_file_is_with_correct_data(self):
        file_content = f'this file must have the same data when downloaded {randint(0, 9999)}'.encode('ascii')
        file_name = f'upload_test_{randint(0, 9999)}'

        self.datalake_connection.upload_file_to_directory(container=self.test_container, path='/', file_name=file_name, data=file_content)

        download = self.datalake_connection.download_file_as_binary(container=self.test_container, path='/', file_name=file_name)

        self.assertEqual(file_content, download)

        self.datalake_connection.delete_directory(container=self.test_container, path=file_name)

    def test_if_uploaded_file_in_bulk_is_with_correct_data(self):
        file_content = f'this file must have the same data when downloaded {randint(0, 9999)}'.encode('ascii')
        file_name = f'upload_test_{randint(0, 9999)}'

        self.datalake_connection.upload_file_to_directory_bulk(container=self.test_container, path='/', file_name=file_name, data=file_content)

        download = self.datalake_connection.download_file_as_binary(container=self.test_container, path='/', file_name=file_name)

        self.assertEqual(file_content, download)

        self.datalake_connection.delete_directory(container=self.test_container, path=file_name)
