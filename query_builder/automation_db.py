__author__ = 'alsherman'

import configparser
from query_builder.connection_string import ConnectionString
from query_builder.database_connector import DatabaseConnector

config = configparser.ConfigParser()
config.read('../config.ini')


class AutomationDb:
    """ Automation database is used to store the list of files that have been successfully loaded. """

    def __init__(self, current_db_files_dict, db_name, table_name):
        """
        param: current_db_files_dict - dict with all new FedScope zipfiles
        """

        self.current_db_files_dict = current_db_files_dict
        self.db_name = db_name
        self.table_name = table_name

    def add_filename_to_existing_files_table(self):
        """ Add all new files to the Automation_DB to confirm that the data has been loaded. """

        # connect to Automation Database
        conn_string = ConnectionString(self.db_name).conn_string
        db = DatabaseConnector(conn_string)
        db.connect_to_db()

        # insert each new file names and url into automation db
        for key, val in self.current_db_files_dict.items():
            query = self.create_automation_db_insert_query(
                        db_name=self.db_name,
                        table_name=self.table_name,
                        values=(key, val))
            db.execute_query(query=query, query_type='select')

    @staticmethod
    def create_automation_db_insert_query(db_name, table_name, values):
        """ create an insert query for each new zipfile after the data is added.

        ARGS: db_name - current db connection
              table_name - table to insert zipfile data
              values - tuple with zipfile name and url

        YIELDS: yield SQL Server insert queries
        """

        insert_query = """USE [{0}]
                           INSERT INTO [dbo].[{1}]
                           ([field_name],[url]) VALUES {2}
                        """.format(db_name, table_name, values)

        return insert_query


