__author__ = 'alsherman'

import logging
import numpy as np
from query_builder.query_builder import QueryBuilder

logger = logging.getLogger(__name__)


class YieldQueries:
    """ create & yield insert queries containing 'row_of_data_per_query' values """

    def __init__(self, selected_file, row_of_data_per_query, range_to_skip=1):
        """
        ARGS: selected_file - selected file to extract from zip
              row_of_data_per_query - number of rows to use in each insert query
              range_to_skip - used if a portion of a file has already been
                              downloaded [default value = 1 means use all data]
        """

        self.selected_file = selected_file
        self.row_of_data_per_query = row_of_data_per_query
        self.range_to_skip = range_to_skip

    def yield_queries(self, custom_header_func=None, custom_skip_criteria=None):
        for ind, row in self.selected_file.yield_row_from_file():
            # skip already inserted rows if db connection breaks
            if 0 < ind < self.range_to_skip:  # default value (1) skips no rows
                 continue

            # apply custom criteria which, if met, skips the row
            if custom_skip_criteria.skip_row(ind=ind, row=row):
                continue

            if not isinstance(row, np.ndarray):
                row=row.split(',')

            query_builder = QueryBuilder(ind=ind,
                                         row=row,
                                         row_of_data_per_query=self.row_of_data_per_query,
                                         selected_file=self.selected_file)

            # first row of data contains the field names
            if ind == 0:
                query_builder.create_fields_for_query(custom_header_func=custom_header_func)
                continue

            # get the values from one row of data and add them to a
            # longer string to insert many values in one query
            query_builder.create_values_for_query()

            # determine if query meets criteria (i.e. includes enough values)
            if query_builder.insert_statement_not_complete and query_builder.not_last_row_of_data:
                continue

            insert_query = query_builder.create_insert_query()
            query_builder.reset_values_for_query()

            # todo: make this output to a different log file as it takes up a lot of space
            logger.info('rows downloaded: {}'.format(ind))

            yield insert_query
