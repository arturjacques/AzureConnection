from connectionazure.tests.unit.unit_base_test import UnitBaseTest
from unittest.mock import Mock, patch
from connectionazure.datalake import ConnectionAzureDataLake
from pandas import DataFrame
import pandas as pd

class MockContainer:
    def __init__(self, name, last_modified):
        self.name = name
        self.last_modified = last_modified

class MockDirectory:
    def __init__(self, permissions, path, last_modified, owner):
        self.permissions = permissions
        self.name = path
        self.last_modified = last_modified
        self.owner = owner

class MockDirectoryList:
    def __init__(self):
        owner = ['john', 'jack', 'jonas']
        last_modified = pd.date_range('2000-01-01', '2020-12-31', periods=3).to_list()
        permissions_list = ['rw-r-----', 'rwxrwxrwx', 'rwxr-----']
        path = ['202105180007', '202105180007/teste', '202105180008']

        self.directory = []
        for file in zip(owner, last_modified, permissions_list, path):
            self.directory.append(MockDirectory(*file))
    
    def get_paths(self, path):
        return self.directory

class ConnectionAzureDataLakeTest(UnitBaseTest):       
    def setUp(self) -> None:
        super().setUp()

        self.datalake_connection = ConnectionAzureDataLake()
        self.datalake_connection.service_client = Mock()

    @patch('connectionazure.datalake.DataLakeServiceClient')
    @patch('connectionazure.datalake.ClientSecretCredential', side_effect = lambda *args: args)
    @patch('connectionazure.datalake.os')
    def test_initialize_storage_account_ad_env_variable(self, mock_os, mock_ClientSecretCredential, mock_DataLakeServiceClient):
        client_id='1234ID'
        client_secret='storage_account_secret'
        teanat_id='tenant_231'
        storage_account_name='datalake'
        
        
        mock_os.environ = {
            "client_id": client_id,
            "client_secret": client_secret,
            "tenant_id": teanat_id,
            "storage_account_name": storage_account_name
        }

        self.datalake_connection.initialize_storage_account_ad_env_variable()

        mock_ClientSecretCredential.assert_called_with(teanat_id, client_id, client_secret)

        mock_DataLakeServiceClient.assert_called_with(account_url=f"https://{storage_account_name}.dfs.core.windows.net", credential=(teanat_id, client_id, client_secret))


    def test_list_directory_contents(self):
        container = 'test_container'
        self.datalake_connection.service_client.get_file_system_client.return_value = MockDirectoryList()
        expected_columns = ['permissions', 'path', 'last_modified', 'owner']

        directory = self.datalake_connection.list_directory_contents(container=container)

        self.assertTrue(len(directory)>=0)
        self.assertEqual(list(directory.columns), expected_columns)
        self.assertEqual(type(directory), DataFrame)
    
    def test_list_containers(self):
        self.datalake_connection.service_client.list_file_systems.return_value = [MockContainer('test_container', '2022-03-04')]
        
        df = self.datalake_connection.list_containers()

        assert self.datalake_connection.service_client.list_file_systems.called is True
        self.assertTrue(type(df)==DataFrame)
        self.assertEqual(list(df.columns), ['container', 'last_modified'])
        
        
    def test_create_and_delete_container(self):
        container_name = 'teste-container'

        self.datalake_connection.service_client = Mock()

        self.datalake_connection.create_container(container_name)
        self.datalake_connection.service_client.create_file_system.assert_called_with(file_system=container_name)

        
    def test_delete_container(self):
        container_name = f'teste-container'

        self.datalake_connection.service_client = Mock()

        self.datalake_connection.delete_container(container_name)
        self.datalake_connection.service_client.delete_file_system.assert_called_with(file_system=container_name)

    def test_create_directory_and_delete(self):
        directory_name = 'folder/inter_folder/teste-folder'
        container = 'test_container'

        self.datalake_connection.service_client = Mock()
        self.datalake_connection.create_directory(container=container, path=directory_name)

        self.datalake_connection.service_client.get_file_system_client.assert_called_with(file_system=container)
        self.datalake_connection.service_client.get_file_system_client().create_directory.assert_called_with(directory_name)

        
    def test_delete_directory(self):
        directory_name = 'folder_delete/inner_folder/test-folder'
        container = 'test_container'

        self.datalake_connection.service_client = Mock()
        self.datalake_connection.delete_directory(container=container, path=directory_name)

        self.datalake_connection.service_client.get_file_system_client.assert_called_with(file_system=container)
        self.datalake_connection.service_client.get_file_system_client().get_directory_clientassert_called_with(directory_name)
        self.datalake_connection.service_client.get_file_system_client().get_directory_client().delete_directory.assert_called()
    

    def test_rename_directory(self):
        new_directory_rename = f'teste-rename.txt'
        container = 'teste_contaier'
        path = 'folder/old_file.txt'
        rename_text = container + '/' + new_directory_rename

        self.datalake_connection.service_client.get_file_system_client().get_directory_client().file_system_name=container

        resp = self.datalake_connection.rename_directory(container=container,
                                                            directory=path,
                                                            new_directory_name=new_directory_rename)

        self.datalake_connection.service_client.get_file_system_client.assert_called_with(file_system=container)
        self.datalake_connection.service_client.get_file_system_client().get_directory_client.assert_called_with(path)
        self.datalake_connection.service_client.get_file_system_client().get_directory_client().rename_directory.assert_called_with(rename_text)

        self.assertTrue(resp)

    @patch('connectionazure.datalake.len')
    def test_check_if_path_exists_false(self, mock_len):
        container = 'test_container'
        path = '/'
        file_name = 'file.txt'

        mock_len.return_value = 3
        paths_df = Mock()
        paths_df.path.values = ['/folder', '/folder/file_not.txt']

        self.datalake_connection.list_directory_contents = Mock()
        self.datalake_connection.list_directory_contents.return_value = paths_df

        resp = self.datalake_connection.check_if_path_exists(container, path, file_name)

        self.assertFalse(resp)
        self.datalake_connection.list_directory_contents.assert_called_with(container=container, path=path)

    @patch('connectionazure.datalake.len')
    def test_check_if_path_exists_true(self, mock_len):
        container = 'test_container'
        path = '/'
        file_name = 'file.txt'


        mock_len.return_value = 3
        paths_df = Mock()
        paths_df.path.values = ['/folder', '/folder/file_not.txt', '/file.txt']

        self.datalake_connection.list_directory_contents = Mock()
        self.datalake_connection.list_directory_contents.return_value = paths_df

        resp = self.datalake_connection.check_if_path_exists(container, path, file_name)

        self.assertTrue(resp)
        self.datalake_connection.list_directory_contents.assert_called_with(container=container, path=path)

    @patch('connectionazure.datalake.len')
    def test_check_if_path_exists_empty(self, mock_len):
        container = 'test_container'
        path = '/'
        file_name = 'file.txt'


        mock_len.return_value = 0
        paths_df = Mock()
        paths_df.path.values = []

        self.datalake_connection.list_directory_contents = Mock()
        self.datalake_connection.list_directory_contents.return_value = paths_df

        resp = self.datalake_connection.check_if_path_exists(container, path, file_name)

        self.assertFalse(resp)
        self.datalake_connection.list_directory_contents.assert_called_with(container=container, path=path)

    def test_upload_file_to_directory_raise_exception(self):
        container = 'test_container'
        file_name = 'teste-file.txt'
        path ='/folder'
        file_content = b'Hello from test upload file to directory'
        error_msg = "/folder/teste-file.txt already exists, can be set overwrite=True to overwrite this file."

        self.datalake_connection.check_if_path_exists = Mock()
        self.datalake_connection.check_if_path_exists.return_value = True

        with self.assertRaises(Exception) as context:
            self.datalake_connection.upload_file_to_directory(container=container,
                                                            path=path,
                                                            file_name=file_name,
                                                            data=file_content)

        self.datalake_connection.check_if_path_exists.assert_called_with(container, path, file_name)
        self.assertEqual(error_msg, context.exception.args[0])

    def test_upload_file_to_directory(self):
        container = 'test_container'
        file_name = 'teste-file.txt'
        path ='/folder'
        file_content = b'Hello from test upload file to directory'


        self.datalake_connection.check_if_path_exists = Mock()
        self.datalake_connection.check_if_path_exists.return_value = False

        self.datalake_connection.upload_file_to_directory(container=container,
                                                        path=path,
                                                        file_name=file_name,
                                                        data=file_content)

        self.datalake_connection.check_if_path_exists.assert_called_with(container, path, file_name)
        self.datalake_connection.service_client.get_file_system_client.assert_called_with(file_system=container)
        (self.datalake_connection
            .service_client.get_file_system_client()
            .get_directory_client.assert_called_with(path))

        (self.datalake_connection
            .service_client.get_file_system_client()
            .get_directory_client()
            .create_file.assert_called_with(file_name))

        (self.datalake_connection
            .service_client.get_file_system_client()
            .get_directory_client()
            .create_file()
            .append_data.assert_called_with(data=file_content, offset=0, length=len(file_content)))

        (self.datalake_connection
            .service_client.get_file_system_client()
            .get_directory_client()
            .create_file()
            .flush_data.assert_called_with(len(file_content)))

    def test_upload_file_to_directory_bulk_raise(self):
        container = 'test_container'
        file_name = 'teste-file.txt'
        path ='/folder'
        file_content = b'Hello from test upload file to directory'
        error_msg = "/folder/teste-file.txt already exists, can be set overwrite=True to overwrite this file."

        self.datalake_connection.check_if_path_exists = Mock()
        self.datalake_connection.check_if_path_exists.return_value = True

        with self.assertRaises(Exception) as context:
            self.datalake_connection.upload_file_to_directory_bulk(container=container,
                                                                    path=path,
                                                                    file_name=file_name,
                                                                    data=file_content)

        self.datalake_connection.check_if_path_exists.assert_called_with(container, path, file_name)
        self.assertEqual(error_msg, context.exception.args[0])

    def test_upload_file_to_directory_bulk(self):
        container = 'test_container'
        file_name = 'teste-file.txt'
        path ='/folder'
        file_content = b'Hello from test upload file to directory'


        self.datalake_connection.check_if_path_exists = Mock()
        self.datalake_connection.check_if_path_exists.return_value = False

        self.datalake_connection.upload_file_to_directory_bulk(container=container,
                                                        path=path,
                                                        file_name=file_name,
                                                        data=file_content)

        self.datalake_connection.check_if_path_exists.assert_called_with(container, path, file_name)
        self.datalake_connection.service_client.get_file_system_client.assert_called_with(file_system=container)
        (self.datalake_connection
            .service_client.get_file_system_client()
            .get_directory_client.assert_called_with(path))

        (self.datalake_connection
            .service_client.get_file_system_client()
            .get_directory_client()
            .create_file.assert_called_with(file_name))

        (self.datalake_connection
            .service_client.get_file_system_client()
            .get_directory_client()
            .create_file()
            .upload_data.assert_called_with(file_content, overwrite=False))

    def test_download_file_as_binary(self):
        container = 'container_test'
        path = 'folder'
        file_name = 'text.txt'
        expected = b'the data is correct on this file'

        self.datalake_connection\
            .service_client\
            .get_file_system_client(file_system=container)\
            .get_directory_client(path)\
            .get_file_client(file_name)\
            .download_file().readall.return_value = expected

        download = self.datalake_connection.download_file_as_binary(container=container,
                                                                    path=path,
                                                                    file_name=file_name)
        
        self.assertEqual(download, expected)
        
        (self.datalake_connection
            .service_client
            .get_file_system_client.assert_called_with(file_system=container))

        (self.datalake_connection
            .service_client
            .get_file_system_client()
            .get_directory_client.assert_called_with(path))

        (self.datalake_connection
            .service_client
            .get_file_system_client()
            .get_directory_client()
            .get_file_client.assert_called_with(file_name))

        (self.datalake_connection
            .service_client
            .get_file_system_client()
            .get_directory_client()
            .get_file_client()
            .download_file.assert_called())

        (self.datalake_connection
            .service_client
            .get_file_system_client()
            .get_directory_client()
            .get_file_client()
            .download_file().readall.assert_called())

    def test_download_file_as_string(self):
        container = 'container_test'
        path = 'folder'
        file_name = 'text.txt'
        returned_bytes = b'the data is correct on this file'
        expected = 'the data is correct on this file'

        self.datalake_connection.download_file_as_binary = Mock()
        self.datalake_connection.download_file_as_binary.return_value = returned_bytes

        download = self.datalake_connection.download_file_as_string(container, path, file_name)

        self.assertEqual(download, expected)
        self.datalake_connection.download_file_as_binary.assert_called_with(container, path, file_name)

    @patch('builtins.print')
    @patch('connectionazure.datalake.read_file_as_bytes')
    @patch('connectionazure.datalake.os')
    def test_move_directory_recursive(self, mock_os, mock_read_file_as_bytes, mock_print):
        upload_file_mock = Mock()
        self.datalake_connection.upload_file_to_directory_bulk = upload_file_mock
        source_directory = 'root_folder'
        container = 'upload_container'
        sink_path = 'container_folder'

        expected_read_calls = ['root_folder/file.txt', 'root_folder/folder/inner_file.txt']
        
        def mock_listdir(folder):
            if folder=='root_folder':
                return ['folder', 'file.txt']
            elif folder=='root_folder/folder':
                return ['inner_file.txt']


        def mock_isdir(directory):
            if directory=='root_folder/folder':
                return True
            else:
                return False

        
        mock_os.listdir = mock_listdir
        mock_os.path.isdir = mock_isdir

        self.datalake_connection.move_directory_recursive(source_directory, container, sink_path)

        mock_read_file_as_bytes.assert_any_call(expected_read_calls[0])
        mock_read_file_as_bytes.assert_any_call(expected_read_calls[1])

        upload_file_mock.assert_any_call(container=container, path=sink_path, file_name='file.txt', data=mock_read_file_as_bytes(), overwrite=True)
        upload_file_mock.assert_any_call(container=container, path=sink_path+'/folder', file_name='inner_file.txt', data=mock_read_file_as_bytes(), overwrite=True)

        mock_print.assert_any_call('root_folder/file.txt copied')
        mock_print.assert_any_call('root_folder/folder/inner_file.txt copied')

