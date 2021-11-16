from tests.unit.unit_base_teste import UnitBaseTest
from connectionazure.datalake import ConnectionAzureDataLake
from pandas import DataFrame

from random import randint

class ConnectionAzureDataLakeTest(UnitBaseTest):       
    def setUp(self) -> None:
        super().setUp()

        self.datalake_connection = ConnectionAzureDataLake()
        self.datalake_connection.initialize_storage_account_ad_env_variable()
        self.test_container = 'test-validation-container'
        self.dummy_rename_file = {'container': self.test_container, 'path':'test_folder/test_rename_file.txt'}

    def test_list_containers(self):
        try:
            container_list = self.datalake_connection.list_containers()
        except:
            container_list = None
        finally:
            self.assertTrue(type(container_list)==DataFrame)
        
    def test_create_and_delete_container(self):
        container_name = f'teste-container-{randint(0, 1000)}'

        self.datalake_connection.create_container(container_name)
        container_created = container_name in self.datalake_connection.list_containers().container.to_list()
        self.assertTrue(container_created)

        self.datalake_connection.delete_container(container_name)
        container_deleted = container_name not in self.datalake_connection.list_containers().container.to_list()
        self.assertTrue(container_deleted)

    def test_list_directory_contents(self):
        directory = self.datalake_connection.list_directory_contents(container=self.test_container)

        self.assertTrue(len(directory)>=0)

    def test_create_directory_and_delete(self):
        directory_name = f'teste-folder-{randint(0, 1000)}'

        self.datalake_connection.create_directory(container=self.test_container, path=directory_name)

        directory_created = directory_name in self.datalake_connection.list_directory_contents(container=self.test_container).path.to_list()

        self.assertTrue(directory_created)
        
        self.datalake_connection.delete_directory(container=self.test_container, path=directory_name)

    def test_rename_directory(self):
        new_directory_rename = f'teste-rename-{randint(0, 1000)}'

        self.datalake_connection.rename_directory(container=self.dummy_rename_file['container'],
                                                  directory=self.dummy_rename_file['path'],
                                                  new_directory_name=new_directory_rename)

        directory_renamed = new_directory_rename in self.datalake_connection.list_directory_contents(container=self.test_container).path.to_list()

        self.assertTrue(directory_renamed)

        self.datalake_connection.rename_directory(container=self.dummy_rename_file['container'],
                                                  directory=new_directory_rename,
                                                  new_directory_name=self.dummy_rename_file['path'])

    
    


        