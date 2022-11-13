import zmq


def write_file(data, filename=None):
    """
    Given data is written to a local file with the given filename

    :param data: A bytes object that stores the file contents
    :param filename: The filename. If not given, a random string is generated 
    :return: The file name of the newly written file, or None in case of an error
    """
    if not filename:
        filename = random_string(length=8)
        filename += ".bin"

    try:
        with open('./'+ filename, 'wb') as f:
            f.write(data)
    except EnvironmentError as e:
        print("Error writing file: {}".format(e))
        return None

    return filename