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
    INTEGER='integer'
    STRING='string'
    DATE='date'
    DATE_TIME='datetime'
    BOOLEAN='boolean'
    NULLBOOLEAN='nullboolean'
    CHOICE='choice'
    MULTICHOICE='multichoice'

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

    def __init__(self, base_table, field_mapping, paths):
        """
        Initialise basic params
        :param base_table: (string) Exact table name as in DB to be used with FROM clause in SQL.
        :param field_mapping: (dict) To map field id's to field name and table name. 
                              Each field id should be mapped to a tuple (field_name, table_name)
        :param paths: (dict) Information about paths from a model to reach to a specific model and when to stop.
        :return: None
        """

        self.base_table = base_table
        self.field_maping = field_mapping
        self.paths = paths

        # Mapping to be used to parse various combination keywords data
        self.WHERE_CONDITION_MAPPING = {
            self.WHERE_CONDITION: self._generate_where_phrase,
            self.AND_CONDITION: self._parse_and,
            self.OR_CONDITION: self._parse_or,
            self.NOT_CONDITION: self._parse_not,
            self.EXISTS_CONDITION: self._parse_exists,
        }

    def generate_sql(self, data, fields):
        """
        Create SQL query from provided json
        :param data: (dict) Actual JSON containing nested condition data
        :param fields: (list) Name of all the fields involved in the conditions data
        :return: (unicode) Finalized SQL query unicode
        """

        join_phrase = self._create_join(fields)
        where_phrase = self._create_where(data)

        return u'SELECT COUNT(*) FROM {base_table} {join_phrase} WHERE {where_phrase}'.format(
            join_phrase=join_phrase,
            base_table=base_table,
            where_phrase=where_phrase
        )

    def _create_join(self, fields):
        """
        Creates join phrase for SQL using the field, field_mapping and joins. 
        Updates _join_names to assign names to each field to be used by _create_where
        :param fields: (list) Fields for which joins need to be created
        :return: (unicode) unicode string that can be appended to SQL just after FROM <table_name>
        """
        raise NotImplementedError

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
