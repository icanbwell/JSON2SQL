from collections import namedtuple


class JSON2SQLGenerator(object):
    """
    To Generate SQL query from JSON data
    """

    # Mapping of field to join name assigned to table
    self._join_names = {}

    # Constants to map JSON keys
    WHERE_CONDITION = 'condition'
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
        between='between'
    )

    DATA_TYPES = namedtuple('DATA_TYPES', [
        'integer', 'string', 'date', 'date_time', 'boolean', 'nullboolean'
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

    def __init__(self, field_mapping, paths):
        """
        Initialise basic params.
        :param field_mapping: (list) List of tuples containing (field_identifier, field_name, table_name).
        :param paths: (list) List of tuples containig (join_table, join_field, parent_table, parent_field).
                      Information about paths from a model to reach to a specific model and when to stop.
        :return: None
        """

        self.field_maping = parse_field_mapping(field_mapping)
        self.paths = parse_path_mapping(paths)
        self.paths = {
            "clients_client": {
                "parent_column": "company_id",
                "parent_table": "accounts_bwelluserclientpremember",
                "child_column": "id"
            },
            "accounts_bwelluserclientpremember": {
                "parent_column": "id",
                "parent_table": "accounts_bwelluser",
                "child_column": "user_id"
            },
            "accounts_bwelluser": {
                "parent_column": "user_id",
                "parent_table": "patients_member",
                "child_column": "id"
            },
            "patients_member": {
                "parent_column": "",
                "parent_table": "",
                "child_column": ""
            }
        }

        # Mapping to be used to parse various combination keywords data
        self.WHERE_CONDITION_MAPPING = {
            self.WHERE_CONDITION: self._generate_where_phrase,
            self.AND_CONDITION: self._parse_and,
            self.OR_CONDITION: self._parse_or,
            self.NOT_CONDITION: self._parse_not,
            self.EXISTS_CONDITION: self._parse_exists,
        }

    def generate_sql(self, data, base_table):
        """
        Create SQL query from provided json
        :param data: (dict) Actual JSON containing nested condition data.
                     Must contain two keys - fields(contains list of fields involved in SQL) and where_data(JSON data)
        :param base_table: (string) Exact table name as in DB to be used with FROM clause in SQL.
        :return: (unicode) Finalized SQL query unicode
        """

        join_phrase = self._create_join(data['fields'])
        where_phrase = self._create_where(data['where_data'])

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
        if table_data['parent_table']:
            query = self._join_member_table(table_data['parent_table'])
            query = 'inner join {parent_table} on {parent_table}.{parent_column} = {child_table}.{child_column} {query}'.format(
                parent_table=table_data['parent_table'],
                parent_column=table_data['parent_column'],
                child_column=table_data['child_column'],
                query=query,
                child_table=table
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
            mapping = next((item for item in self.field_maping if item["id"] == field), None)
            query += self._join_member_table(mapping['table_name'])
        return query

    def _create_where(self, data):
        """
        This function uses recursion to generate sql for nested conditions.
        Every key in the dict will map to a function by referencing WHERE_CONDITION_MAPPING.
        The function mapped to that key will be responsible for generating SQL for that part of the data.
        :param data: (dict) Conditions data which needs to be parsed to generate SQL
        :return: (unicode) Unicode representation of data into SQL
        """
        raise NotImplementedError

    def _generate_where_phrase(self, where):
        """
        Function to generate a single condition(column1 = 1, or column1 BETWEEN 1 and 5) based on data provided.
        Uses _join_names to assign table_name to a field in query.
        :param where: (dict) will contain required data to generate condition. 
                      Sample Format: {"field": , "primary_value": ,"operator": , "secondary_value"(optional): }
        :return: (unicode) SQL condition in unicode represented by where data
        """
        # In this method the main logic will reside for 
        # conveting a given data in the form dict to a actual SQL condtion,
        # which could be added to a where clause in the final SQL
        raise NotImplementedError

    def _parse_and(self, data):
        """
        To parse the AND condition for where clause.
        :param data: (list) contains list of data for conditions that need to be ANDed
        :return: (unicode) unicode containing SQL condition represeted by data ANDed. 
                 This SQL can be directly placed in a SQL query
        """
        raise NotImplementedError

    def _parse_or(self, data):
        """
        To parse the OR condition for where clause.
        :param data: (list) contains list of data for conditions that need to be ORed
        :return: (unicode) unicode containing SQL condition represeted by data ORed. 
                 This SQL can be directly placed in a SQL query
        """
        raise NotImplementedError

    def _parse_exists(self, data):
        """
        To parse the EXISTS check/wrapper for where clause.
        :param data: (dict) contains a nested dict of data for conditions that 
                            need to be wrapped with a EXISTS check in WHERE clause
        :return: (unicode) unicode containing SQL condition represeted by data with EXISTS check. 
                 This SQL can be directly placed in a SQL query
        """
        raise NotImplementedError
   
    def _parse_not(self, data):
        """
        To parse the NOT check/wrapper for where clause.
        :param data: (dict) contains a nested dict of data for conditions that 
                            need to be wrapped with a NOT check in WHERE clause
        :return: (unicode) unicode containing SQL condition represeted by data with NOT check. 
                 This SQL can be directly placed in a SQL query
        """
        raise NotImplementedError
   
    def _parse_conditions(self, condition, data):
        """
        To parse AND, NOT, OR, EXISTS data and
        deligate to proper functions to generate combinations according to condition provided.
        NOTE: This function doesn't do actual parsing. 
              All it does is deligate to a function that would parse the data.
              The main logic for parsing only resides in _generate_where_phrase
              as every condition is similar, its just how we group them
        :param condition: (string) the condition to use to combine the condition represented by data
        :param data: (list) list conditions to be combined or parsed
        :return: (unicode) unicode string that could be placed in the SQL
        """
        raise NotImplementedError
