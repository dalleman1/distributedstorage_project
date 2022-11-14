import platform
import random
import string

def is_raspberry_pi():
    return platform.system() == 'Linux'


def random_string(length=8):
    """
    Returns a random alphanumeric string of the given length. 
    Only lowercase ascii letters and numbers are used.

    :param length: Length of the requested random string 
    :return: The random generated string
    """
    return ''.join([random.SystemRandom().choice(string.ascii_letters + string.digits) for n in range(length)])


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