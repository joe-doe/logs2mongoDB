import time
import json

from threading import Thread
from os import stat
from os.path import abspath
from stat import ST_SIZE


class Log2MongoDB(Thread):
    """
    Create a thread to 'tail -f' the given logfile and store the new
    line to mongodb. Every new line has to be a valid json string.
    Class take care of truncated or temporary missing log files.
    """

    def __init__(self, logfile, mongodb, db_collection):
        """
        Initialize the thread that will be responsible to 'monitor' the logfile
        and store changes to mongodb.

        :param logfile: Source file
        :param mongodb: Destination database object
        :param db_collection: Database collection name
        """
        super(Log2MongoDB, self).__init__()

        print "Store file: {} to mongo collection: {}".format(logfile,
                                                              db_collection)
        self.mongo_instance = mongodb
        self.db_collection = db_collection
        self.logfile = abspath(logfile)
        self.daemon = True
        self.f = None
        self.pos = 0
        self.name = 'mongodb logging in collection '+db_collection

    def _open_file(self):
        """
        Try indefinitely to open the log file. Sleep between intervals.

        :return: file object representing the given log file.
        """
        while True:
            try:
                return open(self.logfile, "r")
            except IOError:
                print "{}: File missing or truncated. " \
                      "Sleep for 5 sec and retry".format(self.logfile)
                time.sleep(5)
                continue

    def _reset(self):
        """
        The file has been truncated or missing, so reopen and get position.
        """
        self.f.close()
        self.f = self._open_file()
        self.pos = self.f.tell()

    def run(self):
        """
        The overridden method of :class:ThreadingThread Class.
        Run indefinitely and store every new line to corresponding
        mongo collection
        """
        self.f = self._open_file()
        file_len = stat(self.logfile)[ST_SIZE]
        self.f.seek(file_len)
        self.pos = self.f.tell()

        while True:
            self.pos = self.f.tell()
            line = self.f.readline()
            if not line:
                try:
                    if stat(self.logfile)[ST_SIZE] < self.pos:
                        # file truncated
                        self._reset()
                    else:
                        time.sleep(1)
                        self.f.seek(self.pos)
                except OSError:
                    # stat failed
                    print "{}: file changed while reading".format(self.logfile)
                    self._reset()
                    continue
            else:
                json_line = json.loads(line)
                # json_line without dots in keys
                # mongodb does not allow dots in key name
                json_l = {k.replace('.', '--'): v
                          for k, v in json_line.items()}
                self.mongo_instance[self.db_collection].insert(json_l)

