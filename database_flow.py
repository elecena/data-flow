from utils import get_kibana, generalize_sql

from data_flow_graph import format_tsv_lines, format_graphviz_lines, logs_map_and_reduce
from sql_metadata import get_query_tables

import logging
import re


def get_flow(period, limit):
    logger = logging.getLogger('get_flow')
    kibana = get_kibana(period)

    # fetch DB queries
    def _map_query(row):
        query = generalize_sql(re.sub(r'^SQL ', '', row['@message']))
        database = row['@fields']['database']['name']

        if database in ['uportal.mysql', 'default']:
            database = 'mysql'

        # print(query, kind, tables)

        return (
            database,
            query,
            'php:{}'.format(row['@context']['method']),
        )

    logs = map(_map_query, kibana.query_by_string('@context.rows: *', limit=limit))
    logs = [log for log in logs if log is not None]

    # print(list(logs))

    # group logs using source name and URL, ignore user agent
    def _map(entry):
        return '{}-{}'.format(entry[0], entry[1])

    # this will be called for each group of logs
    def _reduce(items):
        first = items[0]
        logger.info(first)

        sql = str(first[1])
        tables = get_query_tables(sql) or ['unknown']
        kind = sql.split(' ')[0]
        table = '{}:{}'.format(first[0], tables[0])
        method = first[2]

        ret = {
            'source': table,
            'edge': 'SQL {}'.format(kind),
            'target': method,
            'metadata': '{:.3f} QPS'.format(1. * len(items) / period)
        }

        # reverse the direction of the graph
        # from method (code) to table (database)
        if kind not in ['SELECT']:
            ret['target'] = table
            ret['source'] = method

        return ret

    logger.info('Mapping %d log entries...' % len(logs))
    return logs_map_and_reduce(logs, _map, _reduce)


def main():
    logger = logging.getLogger(__name__)
    graph = get_flow(period=7200, limit=5000)  # last two hours

    logger.info('Saving to TSV...')
    with open('output/database.tsv', 'wt') as fp:
        fp.writelines(format_tsv_lines(graph))

    logger.info('Saving to GV...')
    with open('output/database.gv', 'wt') as fp:
        fp.writelines(format_graphviz_lines(graph))

    logger.info('Done')

if __name__ == "__main__":
    main()
