__author__ = 'alsherman'

import pyodbc
import logging

logger = logging.getLogger(__name__)


class DatabaseConnector:
    def __init__(self, conn_string):
        self.conn_string = conn_string
        self.cursor = None
        self.connection = None

    def __repr__(self):
        return '<class: database connector>'

    def connect_to_db(self):
        """ connect to database

        ARGS: conn_string  - contains the db_name, server_address, user_id, and password
        """

        self.connection = pyodbc.connect(self.conn_string)
        self.cursor = self.connection.cursor()

    def execute_query(self, query, query_type='select'):
        """ execute a provided query

        NOTES: if the database connection is disrupted, then connect_to_db is called

        ARGS:  query - provided query
               query_type - denotes if query is a select or insert
        """

        query_successful = False

        while query_successful is not True:
            try:
                self.cursor.execute(query)
                if query_type == 'insert':
                    self.connection.commit()
                query_successful = True
            except pyodbc.Error as e:
                logger.error('database error: {}'.format(e))
                logger.error('reconnect to database')
                self.connect_to_db()
