from datetime import datetime
from dateutil import tz
from os import getenv

from wikia_common_kibana import Kibana

import logging
import re

logging.basicConfig(level=logging.INFO)


class ElecenaKibana(Kibana):
    @staticmethod
    def format_index(prefix, timestamp, sep='-'):
        """
        :type prefix str
        :type timestamp int
        :type sep str
        :rtype: str
        """
        tz_info = tz.tzutc()

        # e.g.. syslog-ng_2018-05-05
        return "{prefix}_{date}".format(
            prefix=prefix, date=datetime.fromtimestamp(timestamp, tz=tz_info).strftime('%Y-%m-%d'))


def get_kibana(period=3600):
    return ElecenaKibana(es_host=getenv('ES_HOST'), index_prefix='syslog-ng', index_sep='_', period=period)


# borrowed from https://github.com/macbre/query-digest/blob/master/digest/helpers.py
def normalize_likes(sql):
    """
    Normalize and wrap LIKE statements
    :type sql str
    :rtype str
    """
    sql = sql.replace('%', '')

    # LIKE '%bot'
    sql = re.sub(r"LIKE '[^\']+'", 'LIKE X', sql)

    # or all_groups LIKE X or all_groups LIKE X
    matches = re.finditer(r'(or|and) [^\s]+ LIKE X', sql, flags=re.IGNORECASE)
    matches = set([match.group(0) for match in matches]) if matches else None

    if matches:
        for match in matches:
            sql = re.sub(r'(\s?' + re.escape(match) + ')+', ' ' + match + ' ...', sql)

    return sql


def remove_comments_from_sql(sql):
    """
    Removes comments from SQL query
    :type sql str|None
    :rtype str
    """
    return re.sub(r'\s?/\*.+\*/', '', sql)


def generalize_sql(sql):
    """
    Removes most variables from an SQL query and replaces them with X or N for numbers.
    Based on Mediawiki's DatabaseBase::generalizeSQL
    :type sql str|None
    :rtype str
    """
    if sql is None:
        return None

    # multiple spaces
    sql = re.sub(r'\s{2,}', ' ', sql)

    # MW comments
    # e.g. /* CategoryDataService::getMostVisited N.N.N.N */
    sql = remove_comments_from_sql(sql)

    sql = sql.replace('SNIPPETS(', 'SNIPPETS (')
    kind = sql.split(' ')[0].upper()
    if kind in ['UPDATE', 'CALL']:
        sql = ' '.join(sql.split(' ')[:2])

    # handle LIKE statements
    sql = normalize_likes(sql)

    sql = re.sub(r"\\\\", '', sql)
    sql = re.sub(r"\\'", '', sql)
    sql = re.sub(r'\\"', '', sql)
    sql = re.sub(r"'[^\']*'", 'X', sql)
    sql = re.sub(r'"[^\"]*"', 'X', sql)

    # All newlines, tabs, etc replaced by single space
    sql = re.sub(r'\s+', ' ', sql)

    # All numbers => N
    sql = re.sub(r'-?[0-9]+', 'N', sql)

    # WHERE foo IN ('880987','882618','708228','522330')
    sql = re.sub(r' (IN|VALUES)\s*\([^,]+,[^)]+\)', ' \\1 (XYZ)', sql, flags=re.IGNORECASE)

    # added
    sql = re.sub(r'(N,)+', 'N..N', sql)
    sql = re.sub(r'(X,)+', 'X..X', sql)
    sql = re.sub(r'(i:N;)+', 'N..N', sql)
    sql = re.sub(r'WHERE.*', 'WHERE', sql)

    return sql.strip()
