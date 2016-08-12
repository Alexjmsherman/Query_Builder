__author__ = 'alsherman'

import logging

logger = logging.getLogger(__name__)


class QueryBuilder:

    fields_for_query = None
    values_for_query = ''  # used to insert many rows of data for each query

    def __init__(self, ind, row, row_of_data_per_query, selected_file):
        self.ind = ind
        self.row = row
        self.row_of_data_per_query = row_of_data_per_query
        self.file_length = selected_file.file_length
        self.db_name = selected_file.db_name
        self.table_name = selected_file.table_name

    @property
    def insert_statement_not_complete(self):
        # determine if query meets criteria (i.e. includes enough values)
        insert_statement_not_complete = self.ind % self.row_of_data_per_query != 0
        return insert_statement_not_complete

    @property
    def not_last_row_of_data(self):
        not_last_row_of_data = self.ind != self.file_length
        return not_last_row_of_data

    def reset_values_for_query(self):
        QueryBuilder.values_for_query = ''

    def create_fields_for_query(self, custom_header_func=None):
        """ reformat a string of headers for an insert query

        ARGS: row - String of headers

        RETURNS: fields_for_query - string of comma separated fields enclosed in brackets
        """
        if custom_header_func:
            fields_for_query = custom_header_func(self.row)
            QueryBuilder.fields_for_query = fields_for_query
        else:
            fields_for_query = ''
            for field in self.row:
                fields_for_query += '[{}],'.format(field.strip())
            fields_for_query = fields_for_query[:-1]  # remove trailing comma
            QueryBuilder.fields_for_query = fields_for_query

    def create_values_for_query(self):
        """ get values from a dataframe and structure into an insert query

        ARGS: row: String of values from each data row

        RETURNS: String of groups of comma separated values enclosed in parenthesis
        """

        vals = []
        for value in self.row:
            # check for nan values and convert to empty strings
            value = '' if value != value else value

            # error handling for string formats
            try:
                if not isinstance(value, float):
                    value = value.encode('ascii', 'ignore')
            except UnicodeDecodeError:
                value = value.decode('utf-8', 'ignore')
                value = value.encode('ascii', 'ignore')

            value = str(value)

            # replace single quotes as they break the insert queries
            value = value.replace("'",'').strip()

            # replace commas as they break the insert queries
            value = value.replace(',',' ')

            # remove extra quotes wrapping values
            if len(value) > 0 and value[0] == '"' and value[-1] == '"':
                value = value[1:-1]
            vals.append(value)

        values = "('" + "','".join(vals) + "')"
        QueryBuilder.values_for_query += ',' + values

    def create_insert_query(self):
        """ create insert queries

        RETURNS: insert query for current row of data
        """

        # create sql insert query using extracted fields and values
        insert_query =  """USE [{0}]
                           INSERT INTO [dbo].[{1}]
                           ({2}) VALUES {3}
                           """.format(self.db_name,
                                      self.table_name,
                                      QueryBuilder.fields_for_query,
                                      QueryBuilder.values_for_query[1:])  # remove leading comma

        logger.debug("insert query crated: {}".format(insert_query))

        return insert_query