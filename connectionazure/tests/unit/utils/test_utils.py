from os import read
from connectionazure.tests.unit.unit_base_test import UnitBaseTest
from connectionazure.utils import read_file_as_bytes, write_file_as_bytes
from unittest.mock import Mock, patch, mock_open


class UtilsTest(UnitBaseTest):       
    def setUp(self) -> None:
        super().setUp()

    @patch("builtins.open", side_effect=mock_open(), create=True)
    def test_read_file_as_bytes(self, mock_open_file):
        file_path = 'test.txt'

        bytes_returned = read_file_as_bytes(file_path)

        mock_open_file.assert_called_with(file_path, 'rb')

        file_opened = mock_open_file()
        file_opened.read.assert_called()

        self.assertEqual(file_opened.read(), bytes_returned)

    @patch("connectionazure.utils.path")
    @patch("connectionazure.utils.makedirs")
    @patch("builtins.open", side_effect=mock_open(), create=True)
    def test_write_file_as_bytes(self, mock_open_file, mock_makedirs, mock_path):
        file_name = "text.txt"
        file_folder = "folder1/inner_folder"
        file_path = file_folder + '/' + file_name
        binary = b"hello world"
        
        mock_path.isdir.return_value = False

        return_value = write_file_as_bytes(file_path, binary)

        mock_makedirs.assert_called_with(file_folder)
        mock_open_file.assert_called_with(file_path, 'wb')

        file_opened = mock_open_file()
        file_opened.write.assert_called_with(binary)

        self.assertTrue(return_value)

    @patch("connectionazure.utils.path")
    @patch("connectionazure.utils.makedirs")
    @patch("builtins.open", side_effect=mock_open(), create=True)
    def test_write_file_as_bytes_path_exists(self, mock_open_file, mock_makedirs, mock_path):
        file_name = "text.txt"
        file_folder = "folder1/inner_folder"
        file_path = file_folder + '/' + file_name
        binary = b"hello world"
        
        mock_path.isdir.return_value = True

        return_value = write_file_as_bytes(file_path, binary)

        mock_makedirs.assert_not_called()
        mock_open_file.assert_called_with(file_path, 'wb')

        file_opened = mock_open_file()
        file_opened.write.assert_called_with(binary)

        self.assertTrue(return_value)
