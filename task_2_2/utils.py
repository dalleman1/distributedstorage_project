import random
import string
import platform

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
#

def write_file(data, filename=None):
    """
    Write the given data to a local file with the given file name

    :param data: A bytes object that stores the file contents
    :param filename: The file name. If not given, a random string is generated
    :return: The file name of the newly written file, or None if there was an error
    """

    if not filename:
        # Generate random filename
        filename = random_string(8)
        filename += ".bin"

    try:
        # Open filename for writing binary content ('wb')
        # note: when a file is opened using the 'with' statement,
        # it is closed automatically when the scope ends
        with open('./'+filename, 'wb') as f:
            f.write(data)
    except EnvironmentError as e:
        print("Error writing file: {}".format(e))
        return None

    return filename

