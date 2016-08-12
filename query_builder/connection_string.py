__author__ = 'alsherman'

import configparser

config = configparser.ConfigParser()
config.read('../config.ini')


class ConnectionString(object):
    """ connection string used to connect to database """

    def __init__(self, db_name):
        self._db_name = db_name
        self.user_id = config['DATABASE']['USER_ID']
        self.passwd = config['DATABASE']['DB_PASSWORD']
        self.database_type = config['DATABASE']['DATABASE_TYPE']
        self.server = config['DATABASE']['SERVER']
        self.conn_string = db_name

    @property
    def conn_string(self):
        return self._conn_string

    @conn_string.setter
    def conn_string(self, db_name):
        self._conn_string = ''.join(["DRIVER={",self.database_type,
                                     ",};SERVER=",self.server,
                                     ";DATABASE=",db_name,
                                     ";UID=",self.user_id,
                                     ";PWD=",self.passwd,
                                     ";)"])
