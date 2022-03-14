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