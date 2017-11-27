import datetime
import logging
import MySQLdb

from collections import namedtuple

logger = logging.getLogger(u'JSON2SQLGenerator')


class JSON2SQLGenerator(object):
    """
    To Generate SQL query from JSON data
    """

    # Constants to map JSON keys
    WHERE_CONDITION = 'where'
    AND_CONDITION = 'and'
    OR_CONDITION = 'or'
    NOT_CONDITION = 'not'
    EXISTS_CONDITION = 'exists'

    # Supported data types by plugin
    INTEGER = 'integer'
    STRING = 'string'
    DATE = 'date'
    DATE_TIME = 'datetime'
    BOOLEAN = 'boolean'
    NULLBOOLEAN = 'nullboolean'
    CHOICE = 'choice'
    MULTICHOICE = 'multichoice'

    CONVERSION_REQUIRED = [
        STRING, DATE, DATE_TIME
    ]

    # Maintain a set of binary operators
    BETWEEN = 'between'
    BINARY_OPERATORS = (BETWEEN, )

    # Supported operators
    VALUE_OPERATORS = namedtuple('VALUE_OPRATORS', [
        'equals', 'greater_than', 'less_than',
        'greater_than_equals', 'less_than_equals',
        'not_equals', 'is_op', 'in_op', 'like', 'between'
    ])(
        equals='=',
        greater_than='>',
        less_than='<',
        greater_than_equals='>=',
        less_than_equals='<=',
        not_equals='<>',
        is_op='IS',
        in_op='IN',
        like='LIKE',
        between=BETWEEN
    )

    DATA_TYPES = namedtuple('DATA_TYPES', [
        'integer', 'string', 'date', 'date_time', 'boolean', 'nullboolean',
        'choice', 'multichoice'
    ])(
        integer=INTEGER,
        string=STRING,
        date=DATE,
        date_time=DATE_TIME,
        boolean=BOOLEAN,
        nullboolean=NULLBOOLEAN,
        choice=CHOICE,
        multichoice=MULTICHOICE
    )

    # Constants
    FIELD_NAME = 'field_name'
    TABLE_NAME = 'table_name'
    DATA_TYPE = 'data_type'

    JOIN_TABLE = 'join_table'
    JOIN_COLUMN = 'join_column'
    PARENT_TABLE = 'parent_table'
    PARENT_COLUMN = 'parent_column'

    def __init__(self, field_mapping, paths):
        """
        Initialise basic params.
        :param field_mapping: (tuple) tuple of tuples containing (field_identifier, field_name, table_name).
        :param paths: (tuple) tuple of tuples containig (join_table, join_field, parent_table, parent_field).
                      Information about paths from a model to reach to a specific model and when to stop.
        :return: None
        """

        self.field_mapping = self._parse_field_mapping(field_mapping)
        self.paths = self._parse_paths_mapping(paths)

        # Mapping to be used to parse various combination keywords data
        self.WHERE_CONDITION_MAPPING = {
            self.WHERE_CONDITION: '_generate_where_phrase',
            self.AND_CONDITION: '_parse_and',
            self.OR_CONDITION: '_parse_or',
            self.NOT_CONDITION: '_parse_not',
            self.EXISTS_CONDITION: '_parse_exists',
        }

        # Names of the joined tables currently in query in the format ('{table_name}.{field_name}')
        self.joined_table_names = set()

    def generate_sql(self, data, base_table):
        """
        Create SQL query from provided json
        :param data: (dict) Actual JSON containing nested condition data.
                     Must contain two keys - fields(contains list of fields involved in SQL) and where_data(JSON data)
        :param base_table: (string) Exact table name as in DB to be used with FROM clause in SQL.
        :return: (unicode) Finalized SQL query unicode
        """
        self.base_table = base_table
        join_phrase = self._create_join(data['fields'])
        where_phrase = self._create_where(data['where_data'])
        
        # Clear join data
        # TODO: Need to use this variable to actaully store the join data and reuse on future occurances
        self.joined_table_names = set()
        return u'SELECT COUNT(*) FROM {base_table} {join_phrase} WHERE {where_phrase}'.format(
            join_phrase=join_phrase,
            base_table=base_table,
            where_phrase=where_phrase
        )

    def _join_member_table(self, table):
        """
        Function to find member table path from child table
        :param table: child table name
        :return: path from child table to member table.
        """
        table_data = self.paths.get(table)
        query = ''
        if table_data:
            parent_table = table_data[self.PARENT_TABLE]
            join_column = table_data[self.JOIN_COLUMN]
            if '{join_table}.{join_column}'.format(join_table=table, join_column=join_column) not in self.joined_table_names:
                if parent_table != self.base_table:
                    query = self._join_member_table(parent_table)
                query = u'{query} inner join {join_table} on {join_table}.{join_column} = {parent_table}.{parent_column}'.format(
                    parent_table=parent_table,
                    parent_column=table_data[self.PARENT_COLUMN],
                    join_column=join_column,
                    query=query,
                    join_table=table
                )
                self.joined_table_names.add('{join_table}.{join_column}'.format(
                    join_table=table,
                    join_column=table_data[self.JOIN_COLUMN]
                ))
        else:
            logger.error(
                'Table Data not found in paths for table name [{}]'.format(table)
            )
        return query

    def _create_join(self, fields):
        """
        Creates join phrase for SQL using the field, field_mapping and joins. 
        Updates _join_names to assign names to each field to be used by _create_where
        :param fields: (list) Fields for which joins need to be created
        :return: (unicode) unicode string that can be appended to SQL just after FROM <table_name>
        """
        query = ''
        for field in fields:
            table_name = self.field_mapping[field][self.TABLE_NAME]
            if table_name != self.base_table:
                query = u'{0} {1}'.format(query, self._join_member_table(self.field_mapping[field][self.TABLE_NAME]))
        return query.decode('utf-8')

    def _create_where(self, data):
        """
        This function uses recursion to generate sql for nested conditions.
        Every key in the dict will map to a function by referencing WHERE_CONDITION_MAPPING.
        The function mapped to that key will be responsible for generating SQL for that part of the data.
        :param data: (dict) Conditions data which needs to be parsed to generate SQL
        :return: (unicode) Unicode representation of data into SQL
        """
        result = ''
        # Check if data is not blank
        if data:
            # Get the first key in dict.
            condition = data.keys()[0]
            # Call the function mapped to the condition
            function = getattr(self, self.WHERE_CONDITION_MAPPING.get(condition))
            result = function(data[condition])
        return result

    def _get_validated_data(self, where):
        try:
            operator = where['operator'].lower()
            value = where['value']
            field = where['field']
        except KeyError as e:
            raise KeyError(
                u'Missing key - [{}] in where condition dict'.format(e.args[0])
            )
        else:
            # Get optional secondary value
            secondary_value = where.get('secondary_value')
            # Check if secondary_value is present for binary operators
            if operator in self.BINARY_OPERATORS and not secondary_value:
                raise ValueError(
                    u'Missing key - [secondary_value] for operator - [{}]'.format(
                        operator
                    )
                )
            return operator, value, field, secondary_value

    def _generate_where_phrase(self, where):
        """
        Function to generate a single condition(column1 = 1, or column1 BETWEEN 1 and 5) based on data provided.
        Uses _join_names to assign table_name to a field in query.
        :param where: (dict) will contain required data to generate condition. 
                      Sample Format: {"field": , "primary_value": ,"operator": , "secondary_value"(optional): }
        :return: (unicode) SQL condition in unicode represented by where data
        """
        # Check data valid
        if not isinstance(where, dict):
            raise ValueError(
                'Where condition data must be a dict. Found [{}]'.format(
                    type(where)
                )
            )
        # Get all the data elements required and validate them
        operator, value, field, secondary_value = self._get_validated_data(where)
        # Get db field name
        field_name = self.field_mapping[field][self.FIELD_NAME]
        # Get corresponding SQL operator
        sql_operator = getattr(self.VALUE_OPERATORS, operator)
        # Get data type and table name from field_mapping
        data_type = self._get_data_type(field)
        table = self._get_table_name(field)
        # Check if the primary value and data_type are in sync
        self._sanitize_value(value, data_type)
        # Check if the secondary_value and data_type are in sync
        if secondary_value:
            self._sanitize_value(secondary_value, data_type)
        # Make string SQL injection proof
        if data_type == self.STRING:
            self._sql_injection_proof(value)
            if secondary_value:
                self._sql_injection_proof(secondary_value)
        # Make value sql proof. For ex: if value is string or data convert it to '<value>'
        sql_value, secondary_sql_value = self._convert_values(
            [value, secondary_value], data_type
        )
        # Generate SQL phrase
        if sql_operator == self.BETWEEN:
            where_phrase = u'`{table}`.`{field}` {operator} {primary_value} AND {secondary_value}'.format(
                table=table, field=field_name, operator=sql_operator,
                value=sql_value, secondary_value=secondary_sql_value
            )
        else:
            where_phrase = u'`{table}`.`{field}` {operator} {value}'.format(
                operator=sql_operator, table=table, field=field_name, value=sql_value,
            )
        return where_phrase

    def _get_data_type(self, field):
        """
        Gets data type for the field from self.field_mapping configured in __init__
        :param field: (int|string) field identifier that is used as key in self.field_mapping
        :return: (string) data type of the field
        """
        return self.field_mapping[field][self.DATA_TYPE]

    def _get_table_name(self, field):
        """
        Gets table name for the field from self.field_mapping configured in __init__
        :param field: (int|string) Field identifier that is used as key in self.field_mapping
        :return: (string|unicode) Name of the table of the field
        """
        return self.field_mapping[field][self.TABLE_NAME]

    def _convert_values(self, values, data_type):
        """
        Converts values for SQL query. Adds '' string, date, datetime values
        :param values: (iterable) Any instance of iterable values of same data type that need conversion
        :param data_type: (string) Data type of the values provided
        """
        wrapper = '\'{value}\'' if data_type in self.CONVERSION_REQUIRED else '{value}'
        return (wrapper.format(value=value) for value in values)

    def _sanitize_value(self, value, data_type):
        """
        Validate value with data type
        :param value: Values that needs to be validated with data type
        :param data_type: (string) Data type against which the value will be compared
        :return: None
        """
        if data_type == self.INTEGER:
            try:
                int(value)
            except ValueError:
                raise ValueError(
                    'Invalid value -[{}] for data_type - [{}]'.format(
                        value, data_type
                    )
                )
        elif data_type == self.DATE:
            try:
                datetime.datetime.strptime(value, '%Y-%m-%d')
            except ValueError as e:
                raise e
        elif data_type == self.DATE_TIME:
            try:
                datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
            except ValueError as e:
                raise e

    def _parse_and(self, data):
        """
        To parse the AND condition for where clause.
        :param data: (list) contains list of data for conditions that need to be ANDed
        :return: (unicode) unicode containing SQL condition represeted by data ANDed. 
                 This SQL can be directly placed in a SQL query
        """
        return self._parse_conditions(self.AND_CONDITION, data)

    def _parse_or(self, data):
        """
        To parse the OR condition for where clause.
        :param data: (list) contains list of data for conditions that need to be ORed
        :return: (unicode) unicode containing SQL condition represeted by data ORed. 
                 This SQL can be directly placed in a SQL query
        """
        return self._parse_conditions(self.OR_CONDITION, data)

    def _parse_exists(self, data):
        """
        To parse the EXISTS check/wrapper for where clause.
        :param data: (list) contains a list of single element of data for conditions that
                            need to be wrapped with a EXISTS check in WHERE clause
        :return: (unicode) unicode containing SQL condition represeted by data with EXISTS check. 
                 This SQL can be directly placed in a SQL query
        """
        raise NotImplementedError
   
    def _parse_not(self, data):
        """
        To parse the NOT check/wrapper for where clause.
        :param data: (list) contains a list of single element of data for conditions that
                            need to be wrapped with a NOT check in WHERE clause
        :return: (unicode) unicode containing SQL condition represeted by data with NOT check. 
                 This SQL can be directly placed in a SQL query
        """
        return self._parse_conditions(self.NOT_CONDITION, data)
   
    def _parse_conditions(self, condition, data):
        """
        To parse AND, NOT, OR, EXISTS data and
        delegate to proper functions to generate combinations according to condition provided.
        NOTE: This function doesn't do actual parsing. 
              All it does is deligate to a function that would parse the data.
              The main logic for parsing only resides in _generate_where_phrase
              as every condition is similar, its just how we group them
        :param condition: (string) the condition to use to combine the condition represented by data
        :param data: (list) list conditions to be combined or parsed
        :return: (unicode) unicode string that could be placed in the SQL
        """
        sql = bytearray()
        for element in data:
            # Get the first key in the dict.
            inner_condition = element.keys()[0]
            function = getattr(self, self.WHERE_CONDITION_MAPPING.get(inner_condition))
            # Call the function mapped to it.
            result = function(element.get(inner_condition))
            # Append the result to the sql.
            if not sql and condition in [self.AND_CONDITION, self.OR_CONDITION]:
                sql.extend('({})'.format(result))
            else:
                sql.extend(' {0} ({1})'.format(condition, result))
        return u'({})'.format(sql.decode('utf8'))

    def _parse_field_mapping(self, field_mapping):
        """
        Converts tuple of tuples to dict.
        :param field_mapping: (tuple) tuple of tuples in the format ((field_identifier, field_name, table_name, data_type),)
        :return: (dict) dict in the format {'<field_identifier>': {'field_name': <>, 'table_name': <>, 'data_type': <>,}}
        """
        return {
            field[0]: {
               self.FIELD_NAME: field[1],
               self.TABLE_NAME: field[2],
               self.DATA_TYPE: field[3]
            } for field in field_mapping
        }

    def _parse_paths_mapping(self, paths):
        """
        Converts tuple of tuples to dict.
        :param paths: (tuple) tuple of tuples in the format ((join_table, join_field, parent_table, parent_field),)
        :return: (dict) dict in the format {'join_table': {'join_field': , 'parent_table': , 'parent_field': }}
        """
        return {
            path[0]: {
                self.JOIN_COLUMN: path[1],
                self.PARENT_TABLE: path[2],
                self.PARENT_COLUMN: path[3]
            } for path in paths
        }

    def _sql_injection_proof(self, value):
        """
        Escapes strings to avoid SQL injection attacks
        :param value: (string|unicode) string that needs to be escaped
        :return: (string|unicode) escaped string
        """
        return MySQLdb.escape_string(value)
