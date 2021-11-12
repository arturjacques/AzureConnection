from tests.unit.unit_base_teste import UnitBaseTest
from connectionazure.datalake import ConnectionAzureDataLake
from pandas import DataFrame

from random import randint

class ConnectionAzureDataLakeTest(UnitBaseTest):
    def setUp(self) -> None:
        super().setUp()

        self.datalake_connection = ConnectionAzureDataLake()
        self.datalake_connection.initialize_storage_account_ad_env_variable()

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

    


        