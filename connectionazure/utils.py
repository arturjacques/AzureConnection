from os import makedirs, path

def read_file_as_bytes(path):
    """
    read a file as text.

    Parameters
    ----------
    path : str
        path of the file as string
    
    Returns
    -------
    string
        the text that is on file
    """
    with open(path, 'rb') as f:
        binary = f.read()

    return binary

def write_file_as_bytes(path_sink: str, binary: str) -> bool:
    """save binary content in a file

    Args:
        path_sink (str): path were file will be saved
        binary (str): binary that will be saved

    Returns:
        bool: return True if saved with success
    """

    path_sink_split = path_sink.split('/')

    path_folder = '/'.join(path_sink_split[:-1])

    path_is_dir = path.isdir(path_folder)

    if not path_is_dir:
        makedirs(path_folder)

    with open(path_sink, 'wb') as f:
        f.write(binary)

    return True
