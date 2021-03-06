import datetime
import json
import logging
import re

import MySQLdb

from collections import namedtuple, defaultdict

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
    CUSTOM_METHOD_CONDITION = 'custom_method'
    QUESTIONNAIRE_CONDITION = 'questionnaire'

    # Supported data types by plugin
    INTEGER = 'integer'
    STRING = 'string'
    DATE = 'date'
    DATE_TIME = 'datetime'
    BOOLEAN = 'boolean'
    NULLBOOLEAN = 'nullboolean'
    CHOICE = 'choice'
    MULTICHOICE = 'multichoice'
    EXIST = 'exist'

    CONVERSION_REQUIRED = [
        STRING, DATE, DATE_TIME
    ]

    # Boolean Values
    TRUE = 'TRUE'
    FALSE = 'FALSE'

    # Maintain a set of binary operators
    BETWEEN = 'between'
    BINARY_OPERATORS = (BETWEEN, )

    # MySQL aggregate functions
    ALLOWED_AGGREGATE_FUNCTIONS = {'MIN', 'MAX', 'COUNT'}

    # Custom methods field types
    ALLOWED_CUSTOM_METHOD_PARAM_TYPES = {'field', 'integer', 'string', 'date', 'operator', 'boolean', 'variable_template'}

    # Is operator values
    IS_OPERATOR_VALUES_FOR_STRING = {'EMPTY', 'NOT EMPTY'}
    IS_OPERATOR_VALUE = {'NULL', 'NOT NULL', TRUE, FALSE}

    # Is Present operator values
    IS_PRESENT_OPERATOR_VALUE = {TRUE, FALSE}

    # Like operators
    STARTS_WITH = 'starts_with'
    ENDS_WITH = 'ends_with'
    HAS_SUBSTRING = 'has_substring'
    LIKE_OPERATORS = (STARTS_WITH, ENDS_WITH, HAS_SUBSTRING, )

    # Supported dynamic values
    DYNAMIC_DATE = 'DYNAMIC_DATE'
    VARIABLE_TEMPLATE = 'VARIABLE_TEMPLATE'
    DYNAMIC_VALUE_TYPES = (DYNAMIC_DATE, VARIABLE_TEMPLATE, )

    # Dynamic Date Units
    DYNAMIC_DATE_UNITS = {'DAY', 'WEEK', 'MONTH', 'YEAR'}

    # Dynamic Date Operators
    DYNAMIC_DATE_OPERATORS = namedtuple('DYNAMIC_DATE_OPERATORS', [
        'date_sub', 'date_add'
    ])(
        date_sub='DATE_SUB',
        date_add='DATE_ADD',
    )

    # Supported operators
    VALUE_OPERATORS = namedtuple('VALUE_OPERATORS', [
        'equals', 'greater_than', 'less_than',
        'greater_than_equals', 'less_than_equals',
        'not_equals', 'is_op', 'in_op', 'like', 'between',
        'is_challenge_completed', 'is_challenge_not_completed',
        'starts_with', 'ends_with', 'has_substring', 'verifies_regex',
        'is_present',
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
        is_challenge_completed='is_challenge_completed',
        is_challenge_not_completed='is_challenge_not_completed',
        between=BETWEEN,
        starts_with='LIKE',
        ends_with='LIKE',
        has_substring='LIKE',
        verifies_regex='REGEXP',
        is_present='is_present',
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
    JOIN_TABLE_ACTIVE_FIELD = 'join_table_active_field'
    PARENT_TABLE = 'parent_table'
    PARENT_COLUMN = 'parent_column'

    CHALLENGE_CHECK_QUERY = 'EXISTS (SELECT 1 FROM journeys_memberstagechallenge WHERE challenge_id = {value} AND ' \
                            'completed_date IS NOT NULL AND member_id = patients_member.id) '

    # - Used in custom method mapping -
    TEMPLATE_STR_KEY = 'template_str'
    TEMPLATE_PARAMS_KEY = 'parameters'
    TEMPLATE_KEY_REGEX = r"{(\w+)}"

    # - Used in subquery mapping -
    SUBQUERY_STR_KEY = 'subquery_template_str'
    SUBQUERY_PARAMS_KEY = 'subquery_params'
    SUBQUERY_FIELDS_KEY = 'subquery_fields'
    SUBQUERY_IS_SQL = 'subquery_is_sql'

    # - Used in variable templates mapping -
    VARIABLE_TEMPLATE_KEYWORD = 'variable_template_keyword'
    VARIABLE_TEMPLATE_RETURN_TYPE = 'variable_template_return_type'

    def __init__(self, data):
        """
        Initialise basic params.
        : param data: (dict) dict containing following keys:
                        custom_methods: (tuple) tuple of tuples containing (id, sql_template, variables)
                        field_mapping: (tuple) tuple of tuples containing (field_identifier, field_name, table_name).
                        paths: (tuple) tuple of tuples containing (join_table, join_field, parent_table, parent_field).
                                Information about paths from a model to reach to a specific model and when to stop.
                        subqueries: (tuple) tuple of tuples containing (id, is_sql, template, fields, parameters).
        :return: None
        """
        assert 'field_mapping' in data, 'Field mapping key is required in data when initializing params'
        assert 'paths' in data, 'Paths key is required in data when initializing params'
        assert 'custom_methods' in data, 'Custom Methods key is required in data when initializing params'
        assert 'subqueries' in data, 'Subqueries key is required in data when initializing params'
        assert 'variable_templates' in data, 'Variable Templates key is required in data when initializing params'

        self.base_table = ''
        self.field_mapping = self._parse_field_mapping(data.get('field_mapping'))
        self.path_mapping = self._parse_multi_path_mapping(data.get('paths'))
        self.custom_methods = self._validate_custom_methods(data.get('custom_methods'))
        self.subquery_mapping = self._parse_subquery_mapping(data.get('subqueries'))
        self.variable_templates = self._parse_variable_templates(data.get('variable_templates'))

        # Mapping to be used to parse various combination keywords data
        self.WHERE_CONDITION_MAPPING = {
            self.WHERE_CONDITION: '_generate_where_phrase',
            self.AND_CONDITION: '_parse_and',
            self.OR_CONDITION: '_parse_or',
            self.NOT_CONDITION: '_parse_not',
            self.EXISTS_CONDITION: '_parse_exists',
            self.CUSTOM_METHOD_CONDITION: '_parse_custom_method_condition',
            # Mapping questionnaire to custom method condition as we are using custom methods SQL
            # for supporting questionnaire
            self.QUESTIONNAIRE_CONDITION: '_parse_custom_method_condition',
        }

        self.DYNAMIC_VALUE_MAPPING = {
            self.DYNAMIC_DATE: '_generate_dynamic_date',
            self.VARIABLE_TEMPLATE: '_generate_variable_template'
        }

    def _validate_custom_methods(self, sql_templates):
        """
        Validate the template data and pre process the data.

        :param sql_templates: (tuple) tuple of tuples containing (id, sql_template, variables)
        :return: (dict) { template_id: { template_str:, template_parameters: }
        """
        template_mapping = {}

        for template_id, template_str, parameters in sql_templates:
            parameters = json.loads(parameters)
            template_str = template_str.strip()

            assert template_str, 'Not a valid template string'
            assert template_id not in template_mapping, 'Template id must be unique'
            template_defined_variables = set(re.findall(self.TEMPLATE_KEY_REGEX, template_str, re.MULTILINE))
            # Checks if variable defined in template string and variables declared are exactly same
            assert not set(parameters.keys()) ^ template_defined_variables, 'Extra variable defined'
            # Checks parameter types are permitted
            assert not {
                l['data_type'] for l in parameters.values()
            } - self.ALLOWED_CUSTOM_METHOD_PARAM_TYPES, 'Invalid data type defined'

            template_mapping[template_id] = {
                self.TEMPLATE_STR_KEY: template_str,
                self.TEMPLATE_PARAMS_KEY: parameters
            }

        return template_mapping

    def _validate_subquery(self, subquery):
        """
        Validate the sub-query data.
        :param subquery: (dict) Sub-Query dict containing (id, template, fields, parameters, is_sql)
        :return:
        """
        assert isinstance(
            subquery[self.SUBQUERY_FIELDS_KEY], dict
        ), 'Sub-Query fields is not a valid json data'
        if subquery[self.SUBQUERY_IS_SQL]:
            template_variables = set(
                re.findall(self.TEMPLATE_KEY_REGEX, subquery[self.SUBQUERY_STR_KEY], re.MULTILINE)
            )
            if template_variables:
                parameters = subquery[self.SUBQUERY_PARAMS_KEY]
                assert isinstance(parameters, dict), 'Sub-Query parameters is not a valid json data'
                # Checks if variable defined in template string and variables declared are exactly same
                assert not set(parameters.keys()) ^ template_variables, 'Extra variable defined'
                # Checks parameter types are permitted
                assert not {
                    l['data_type'] for l in parameters.values()
                } - self.ALLOWED_CUSTOM_METHOD_PARAM_TYPES, 'Invalid data type defined'
        else:
            assert isinstance(
                subquery[self.SUBQUERY_STR_KEY], dict
            ), 'Sub-Query template is not a valid json data'

    def _parse_subquery_mapping(self, subqueries):
        """
        Validate the template data and pre process the data.

        :param subqueries: (tuple) tuple of tuples containing (id, sql_template, variables)
        :return: (dict) { template_id: { template_str:, template_parameters: }
        """
        subquery_mapping = {}
        for subquery_id, is_sql, template_str, fields, parameters in subqueries:
            parameters = json.loads(parameters)
            fields = json.loads(fields)

            if not is_sql:
                template_str = json.loads(template_str)
            assert subquery_id not in subquery_mapping, 'Subquery id must be unique'

            subquery_mapping[subquery_id] = {
                self.SUBQUERY_STR_KEY: template_str,
                self.SUBQUERY_PARAMS_KEY: parameters,
                self.SUBQUERY_FIELDS_KEY: fields,
                self.SUBQUERY_IS_SQL: is_sql
            }
        return subquery_mapping

    def _parse_variable_templates(self, variable_templates):
        """
        Converts tuple of tuples to dict.
        :param variable_templates: (tuple) tuple of tuples containing (unique_id, keyword, return_type)
        :return: (dict) { unique_id: { variable_template_keyword:<value>, variable_template_return_type:<value> }
        """
        return {
            str(unique_id): {
                self.VARIABLE_TEMPLATE_KEYWORD: keyword,
                self.VARIABLE_TEMPLATE_RETURN_TYPE: return_type,
            } for unique_id, keyword, return_type in variable_templates
        }

    def _parse_custom_method_condition(self, data):
        """
        Process the custom method condition to render SQL template using the arguments given.

        :param data: (dict) Expect dict of custom methods of format {template_id:, parameters: }
        :return:
        """
        assert isinstance(data, dict), 'Input data must be a dict'
        assert 'template_id' in data, 'No template_id is provided'
        template_id = data['template_id']
        template_data = self.custom_methods[template_id]

        # Process parameters
        validated_parameters = {}
        for param_id, param_data in data.get('parameters', {}).items():
            assert param_id in template_data[self.TEMPLATE_PARAMS_KEY], 'Invalid parameter name.'
            param_type = template_data[self.TEMPLATE_PARAMS_KEY][param_id]['data_type']

            validated_parameters[param_id] = self._process_parameter(param_type, param_data)

        # Check that we have collected all the required keys
        template_params = template_data[self.TEMPLATE_PARAMS_KEY].keys()
        assert len(set(template_params) ^ set(validated_parameters.keys())) == 0, \
            'Missing or extra template variable'

        return template_data[self.TEMPLATE_STR_KEY].format(**validated_parameters)

    def _process_parameter(self, data_type, parameter_data):
        assert len(data_type) > 0, 'Invalid data type'
        assert isinstance(parameter_data, dict), 'Invalid parameter data format'

        value = parameter_data.get('value')
        if value:
            data_type_upper = data_type.upper()
            if data_type_upper != 'DATE':
                self._sanitize_value(value, data_type.lower())
            if data_type_upper == 'FIELD':
                field_data = self.field_mapping[parameter_data['field']]
                return "`{table}`.`{field}`".format(
                    table=field_data[self.TABLE_NAME], field=field_data[self.FIELD_NAME]
                )
            elif data_type_upper == 'INTEGER':
                return int(value)
            elif data_type_upper == 'STRING':
                return "'{value}'".format(value=self._sql_injection_proof(value))
            elif data_type_upper == 'DATE':
                return self._get_sql_value(value, data_type)
            elif data_type_upper == 'OPERATOR':
                return getattr(self.VALUE_OPERATORS, value)
            elif data_type_upper == 'BOOLEAN':
                value = value.upper()
                assert value in self.IS_OPERATOR_VALUE, 'Invalid value for boolean type'
                return value
            elif data_type_upper == 'VARIABLE_TEMPLATE':
                return '{{{value}}}'.format(value=self._sql_injection_proof(value))
            else:
                raise AttributeError(
                    "Unsupported data type for parameter: {type}".format(type=data_type)
                )

    def _generate_alias_params(self, subqueries):
        """
        Creates dict of alias and it's params so that we can use it to create sql
        :param subqueries: subqueries data passed in an eligibility json
        :return: (dict) Dict containing all alias and there params
        """
        alias_params = {}
        for subquery in subqueries:
            assert 'alias' in subquery, 'Alias is not present'
            alias = subquery.get('alias')
            alias_params[alias] = subquery.get('parameters', {})
        return alias_params

    def generate_sql(self, data, base_table, **kwargs):
        """
        Create SQL query from provided json
        :param data: (dict) Actual JSON containing nested condition data.
                     Must contain two keys - fields(contains list of fields involved in SQL) and where_data(JSON data)
        :param base_table: (string) Exact table name as in DB to be used with FROM clause in SQL.
        :param select_fields: (dict) JSON containing select fields
        :return: (unicode) Finalized SQL query unicode
        """

        self.base_table = base_table
        assert self.validate_where_data(data.get('where_data', {})), 'Invalid where data'
        where_phrase = self._generate_sql_condition(data['where_data'])

        if 'additional_where_clause' in kwargs:
            where_phrase = where_phrase + kwargs['additional_where_clause']

        if 'group_by_fields' in data:
            assert isinstance(data['group_by_fields'], list), 'Group by fields need to list of dict'
            data['group_by_fields'] = [x['field'] for x in data['group_by_fields']]

        path_subset = self.extract_paths_subset(
            [self.field_mapping[field_id][self.TABLE_NAME] for field_id in data['fields']],
            data.get('path_hints', {})
        )
        join_tables = self.create_join_path(path_subset, self.base_table)
        join_phrase = self.generate_left_join(join_tables)
        group_by_phrase = self.generate_group_by(
            data.get('group_by_fields', []), data.get('having', {})
        )
        alias_params = None
        if 'alias_params' not in kwargs:
            alias_params = self._generate_alias_params(data.get('sub_queries', []))
        sub_query_phrase = self.generate_subquery(
            data.get('sub_queries', []), kwargs.get('alias_params', alias_params)
        )
        select_phrase = self.generate_select_phrase(kwargs.get('select_fields'))

        return u'SELECT {select_phrase} FROM {base_table} {sub_query_phrase} {join_phrase}' \
               u' WHERE {where_phrase} {group_by_fragment}'.format(
                   join_phrase=join_phrase,
                   base_table=base_table,
                   where_phrase=where_phrase,
                   group_by_fragment=group_by_phrase,
                   select_phrase=select_phrase,
                   sub_query_phrase=sub_query_phrase
               )

    def _parse_multi_path_mapping(self, paths):
        """
        Create mapping of what nodes can be reached from any given node.
        This method also support the case when you can jump to multiple node from any given node

        :param paths: (tuple) tuple of tuples in the format ((join_table, join_field, parent_table, parent_field),)
        :return: (dict) dict in the format {'join_table': {'parent_table': {'parent_field': , 'join_field':} }}
        """
        path_map = defaultdict(dict)
        for join_tbl, join_fld, parent_tbl, parent_fld, join_tbl_active_fld in paths:
            # We can support if there are multiple ways to join a table
            # We don't support if there are multiple fields on join table path
            assert parent_tbl not in path_map[join_tbl], 'Joins with multiple fields is not supported'
            path_map[join_tbl][parent_tbl] = {
                self.PARENT_COLUMN: parent_fld,
                self.JOIN_COLUMN: join_fld,
                self.JOIN_TABLE_ACTIVE_FIELD: join_tbl_active_fld,
            }

        return path_map

    def extract_paths_subset(self, start_nodes, path_hints):
        """
        Extract a subset of paths which only contains paths which are possible from starting nodes
        When there is multiple options from any node then we look in path hints to select a node.

        This method also aims to merge duplicate path nodes.
        Example:
        A -> B -> C
        A -> B ->  D

        Merge this into
        A -> B -> C
             | -> D

        Merge happens from left side not right side.
        As our current implementation base is always on left side

        Left side is always base_table
        :param start_nodes: Array of table names
        :param path_hints:
        :return:
        """
        path_subset = defaultdict(set)
        # Convert start nodes to set as we would need this for lookups
        start_nodes = set(start_nodes)
        traversal_nodes = list(start_nodes)  # type: list

        # We would be doing traversal from given tables towards base tables.
        while traversal_nodes:
            curr_node = traversal_nodes.pop()  # type: str

            # This condition indicate that we have reached end of path
            if curr_node == self.base_table:
                continue

            next_nodes = self.path_mapping[curr_node]  # type: dict

            if curr_node in path_hints:
                assert path_hints[curr_node] in next_nodes, 'Node provided in hint is not a valid option.'
                assert len(
                    set(self.path_mapping[curr_node]) &
                    (start_nodes | set(path_hints.values()))
                ) == 1, 'Multiple paths are selected from node {curr_node}'.format(curr_node=curr_node)
                parent_node = path_hints[curr_node]
                traversal_nodes.append(parent_node)
            elif len(next_nodes) == 1:
                parent_node = list(next_nodes.keys())[0]
                traversal_nodes.append(parent_node)
            else:
                raise Exception("No path hint provided for `{curr_node}`".format(curr_node=curr_node))

            path_subset[parent_node].add(curr_node)

        return path_subset

    def create_join_path(self, path_map, curr_table):
        """
        Convert the path subset into a join table
        Return list of tuples
        [(join table, parent table)]
        :param path_map: (dict) Nested dict of format { join_table: { parent_table: {join_field, parent_field} } }
        :param curr_table: (str) Node from which we need to be created.
        :return:
        """
        # This condition satisfied when we reach the end of the current path
        if curr_table not in path_map:
            return

        for table_name in sorted(path_map[curr_table]):
            yield (table_name, curr_table)
            for item in self.create_join_path(path_map, table_name):
                yield item

    def generate_left_join(self, join_path):
        join_phrases = []
        for join_table, parent_table in join_path:
            join_condition = '{join_tbl}.{join_fld} = {parent_tbl}.{parent_fld}'.format(
                join_tbl=join_table,
                parent_tbl=parent_table,
                join_fld=self.path_mapping[join_table][parent_table][self.JOIN_COLUMN],
                parent_fld=self.path_mapping[join_table][parent_table][self.PARENT_COLUMN]
            )
            join_table_active_field = self.path_mapping[join_table][parent_table][self.JOIN_TABLE_ACTIVE_FIELD]
            # If join table has a field which specifies if row is soft deleted or not then add it in join condition
            if join_table_active_field:
                join_condition = '({join_condition} AND {join_tbl}.{join_table_active_field} = TRUE)'.format(
                    join_condition=join_condition, join_tbl=join_table, join_table_active_field=join_table_active_field
                )
            join_phrases.append(
                'LEFT JOIN {join_tbl} ON {join_condition}'.format(join_tbl=join_table, join_condition=join_condition)
            )

        return ' '.join(join_phrases)

    def generate_group_by(self, group_by_fields, having_clause):
        """
        Validate and return group by and having clause statement

        :rtype: str
        :type having_clause: Dict
        :type group_by_fields: List[int]
        """
        assert isinstance(group_by_fields, list)
        assert isinstance(having_clause, dict)

        assert self.validate_group_by_data(group_by_fields, having_clause), 'Invalid having data'

        if not group_by_fields:
            return ''

        result = ''
        fully_qualified_field_names = [
            '`{table_name}`.`{field_name}`'.format(
                table_name=self.field_mapping[field_id][self.TABLE_NAME],
                field_name=self.field_mapping[field_id][self.FIELD_NAME]
            )
            for field_id in group_by_fields
        ]

        result += 'GROUP BY {fields}'.format(fields=', '.join(fully_qualified_field_names))
        if list(having_clause.keys()):
            result += ' HAVING {condition}'.format(condition=self._generate_sql_condition(having_clause))

        return result

    def generate_subquery(self, subqueries, alias_params):
        result = []
        for subquery_dict in subqueries:
            if 'unique_id' in subquery_dict:
                subquery = self.subquery_mapping[subquery_dict['unique_id']]

                # Validate given subquery
                self._validate_subquery(subquery)

                # Check if alias for the subquery is present
                assert 'alias' in subquery_dict, 'Alias is not present'
                alias = subquery_dict.get('alias')

                select_fields = subquery[self.SUBQUERY_FIELDS_KEY]
                join_fld = None
                for select_field_id, select_field_data in select_fields.items():
                    if select_field_data.get('is_member_id'):
                        assert 'alias' in select_field_data, 'Alias is required for {id} field'.format(
                            id=select_field_id
                        )
                        join_fld = select_field_data.get('alias')

                if subquery[self.SUBQUERY_IS_SQL]:
                    # Process parameters
                    validated_parameters = {}
                    for param_id, param_data in alias_params.get(alias, {}).items():
                        assert param_id in subquery[self.SUBQUERY_PARAMS_KEY], 'Invalid parameter name.'
                        param_type = subquery[self.SUBQUERY_PARAMS_KEY][param_id]['data_type']

                        validated_parameters[param_id] = self._process_parameter(param_type, param_data)
                    sql = subquery[self.SUBQUERY_STR_KEY].format(**validated_parameters)
                    assert join_fld is not None, 'Member id mapping is required in the subquery'
                else:
                    if not join_fld:
                        join_fld = 'member_id'
                    sql = self.generate_sql(
                        subquery[self.SUBQUERY_STR_KEY], self.base_table,
                        **{'select_fields': select_fields, 'alias_params': alias_params}
                    )
                result.append(
                    'LEFT JOIN ( {sql} ) AS {alias} ON `{join_tbl}`.`{join_fld}` = `{parent_tbl}`.`id`'.format(
                        sql=sql, alias=alias, join_tbl=alias, join_fld=join_fld, parent_tbl=self.base_table
                    )
                )
        return ' '.join(result)

    def generate_select_phrase(self, select_fields=None):
        """
        Function to create select phrase for a sql
        :param select_fields: (dict) JSON which contains the select fields
        :return: (unicode) select fields for a SQL
        """
        if select_fields:
            select_phrase = []
            for select_field_alias, select_field_data in select_fields.items():
                if select_field_alias != 'member_id':
                    field_name = self.field_mapping[select_field_data['field']][self.FIELD_NAME]
                    table = self._get_table_name(select_field_data['field'])
                else:
                    field_name = select_field_data['field']
                    table = select_field_data['category']
                select_field = '`{table}`.`{field_name}`'.format(table=table, field_name=field_name)

                # Apply aggregate function to select fields
                if 'aggregate_lhs' in select_field_data and select_field_data.get('aggregate_lhs'):
                    aggregate_func_name = select_field_data['aggregate_lhs'].upper()  # type: unicode
                    assert aggregate_func_name in self.ALLOWED_AGGREGATE_FUNCTIONS, \
                        'Unsupported aggregate functions: {}'.format(aggregate_func_name)
                    select_field = '{func_name}({field_name})'.format(
                        func_name=aggregate_func_name, field_name=select_field
                    )

                assert 'alias' in select_field_data, 'Alias name is missing for {select_field} ' \
                                                     'select field in subquery'.format(select_field=select_field)
                select_field = self._sql_injection_proof(select_field)
                alias = self._sql_injection_proof(select_field_data['alias'])
                select_phrase.append('{select_field} AS {alias}'.format(
                    select_field=select_field, alias=alias
                ))
            return ', '.join(select_phrase)
        else:
            return 'COUNT(DISTINCT `{base_table}`.`id`)'.format(base_table=self.base_table)

    def validate_group_by_data(self, group_by_fields, having):
        """
        Validate the group by data to check if it can produce a query which is valid.
        For example it would check only group by fields or aggregate functions are being used.

        :type having: Dict
        :type group_by_fields: List[int]
        """
        assert isinstance(group_by_fields, list)
        assert isinstance(having, dict)

        for cond in self.extract_key_from_nested_dict(having, self.WHERE_CONDITION):
            assert isinstance(cond, dict), 'where condition needs to be dict'
            assert 'aggregate_lhs' in cond or cond.get('field') in group_by_fields, \
                'Use of non aggregate value or non grouped field: {cond}'.format(cond=cond)

        return True

    def validate_where_data(self, where_data):
        """
        Validate if where fields doesn't contains use of aggregation function
        :type where_data: Dict
        """
        assert isinstance(where_data, dict) and len(where_data) > 0, \
            'Invalid or empty where data'

        for cond in self.extract_key_from_nested_dict(where_data, self.WHERE_CONDITION):
            assert isinstance(cond, dict), 'Invalid where condition'
            assert cond.get('aggregate_lhs', '') == '', \
                'Use of non aggregate value or non grouped field: {cond}'.format(cond=cond)

        return True

    def _generate_sql_condition(self, data):
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
            condition = list(data.keys())[0]
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
                u'Missing key - [{key}] in where condition dict'.format(key=e.args[0])
            )
        else:
            # Get optional secondary value
            secondary_value = where.get('secondary_value')
            # Check if secondary_value is present for binary operators
            if operator in self.BINARY_OPERATORS and not secondary_value:
                raise ValueError(
                    u'Missing key - [secondary_value] for operator - [{operator}]'.format(
                        operator=operator
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
                'Where condition data must be a dict. Found [{where_type}]'.format(
                    where_type=type(where)
                )
            )
        # Get all the data elements required and validate them
        operator, value, field, secondary_value = self._get_validated_data(where)
        # Get corresponding SQL operator
        sql_operator = getattr(self.VALUE_OPERATORS, operator)
        if 'subquery' in where:
            subquery = self.subquery_mapping[where.get('subquery')]
            select_fields = subquery.get(self.SUBQUERY_FIELDS_KEY)
            subquery_select_field = select_fields.get(field)
            # Get alias db field name from where data
            field_name = subquery_select_field.get('alias')
            # Get alias table name from where data
            table = where.get('alias')
            data_type = subquery_select_field.get('data_type')
        else:
            # Get db field name from field_mapping
            field_name = self.field_mapping[field][self.FIELD_NAME]
            # Get table name from field_mapping
            table = self._get_table_name(field)
            # Get data type from field_mapping
            data_type = self._get_data_type(field)

        # `value` contains the R.H.S part of the equation.
        # In case of `IS` operator R.H.S can be `NULL` or `NOT NULL`
        # irrespective of data type of the L.H.S.
        # Hence we want to skip data type check for `IS` operator.
        if sql_operator == self.VALUE_OPERATORS.is_op:
            value_in_upper_case = value.upper()
            if data_type == self.STRING:
                assert value_in_upper_case in self.IS_OPERATOR_VALUES_FOR_STRING, 'Invalid rhs for `IS` operator'
                sql_operator = (
                    self.VALUE_OPERATORS.not_equals if 'NOT' in value_in_upper_case
                    else self.VALUE_OPERATORS.equals
                )
                value = "''"
            else:
                assert value_in_upper_case in self.IS_OPERATOR_VALUE, 'Invalid rhs for `IS` operator'
            sql_value, secondary_sql_value = value, None
        else:
            # Update value if operator is in like operators
            if data_type == self.STRING and operator in self.LIKE_OPERATORS:
                if operator == self.STARTS_WITH:
                    like_value = '{value}%%'
                elif operator == self.ENDS_WITH:
                    like_value = '%%{value}'
                else:
                    like_value = '%%{value}%%'
                value = like_value.format(value=value)

            sql_value = self._get_sql_value(value, data_type)
            secondary_sql_value = self._get_sql_value(secondary_value, data_type)

        lhs = u'`{table}`.`{field}`'.format(table=table, field=field_name)  # type: unicode

        # Apply aggregate function to L.H.S
        if 'aggregate_lhs' in where and where['aggregate_lhs']:
            aggregate_func_name = where['aggregate_lhs'].upper()  # type: unicode
            if aggregate_func_name in self.ALLOWED_AGGREGATE_FUNCTIONS:
                lhs = u'{func_name}({field_name})'.format(func_name=aggregate_func_name, field_name=lhs)
            else:
                logger.info('Unsupported aggregate functions: %s', aggregate_func_name)

        # TODO: Based on the assumption that below operator will only used
        #           with challenge.
        if sql_operator in [self.VALUE_OPERATORS.is_challenge_completed,
                            self.VALUE_OPERATORS.is_challenge_not_completed]:
            return "{negate} {check}".format(
                negate=(
                    'NOT' if sql_operator == self.VALUE_OPERATORS.is_challenge_not_completed
                    else ''
                ),
                check=self.CHALLENGE_CHECK_QUERY.format(value=sql_value)
            )

        # Generate SQL phrase for is_present value operator
        if sql_operator == self.VALUE_OPERATORS.is_present:
            value_in_upper_case = sql_value.upper()
            # Strip quotes if data type is choice
            if data_type == self.CHOICE:
                value_in_upper_case = value_in_upper_case.strip('\'')
            assert value_in_upper_case in self.IS_PRESENT_OPERATOR_VALUE, 'Invalid rhs for `is_present` operator'
            is_present = value_in_upper_case == self.TRUE
            return "{lhs} IS {null_negate}NULL {operator} {lhs} {empty_negate}= ''".format(
                lhs=lhs, null_negate='NOT ' if is_present else '',
                empty_negate='!' if is_present else '',
                operator=self.AND_CONDITION if is_present else self.OR_CONDITION
            )

        # Generate SQL phrase
        if sql_operator == self.BETWEEN:
            where_phrase = u'{lhs} {operator} {primary_value} AND {secondary_value}'.format(
                lhs=lhs, operator=sql_operator,
                primary_value=sql_value, secondary_value=secondary_sql_value
            )
        else:
            where_phrase = u'{lhs} {operator} {value}'.format(
                operator=sql_operator, lhs=lhs, value=sql_value,
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
        Converts values for SQL query. Adds '' string, date, datetime values, string choice value
        :param values: (iterable) Any instance of iterable values of same data type that need conversion
        :param data_type: (string) Data type of the values provided
        """
        if data_type in [self.CHOICE, self.MULTICHOICE]:
            # try converting the value to int
            try:
                int(values[0])
            except ValueError:
                wrapper = '\'{value}\''
            else:
                wrapper = '{value}'
        elif data_type in self.CONVERSION_REQUIRED:
            wrapper = '\'{value}\''
        else:
            wrapper = '{value}'
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
                    'Invalid value -[{value}] for data_type - [{data_type}]'.format(
                        value=value, data_type=data_type
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
                try:
                    datetime.datetime.strptime(value, '%Y-%m-%d')
                except ValueError as e:
                    raise e

    def _validate_sql_values(self, value, data_type):
        """
        Validate value for where condition
        :param value: (String) Value to be validated
        :param data_type: (String) Data type of value
        :return: Validated value
        """
        if value and not isinstance(value, dict):
            # Check if the primary value and data_type are in sync
            self._sanitize_value(value, data_type)
            # Make string SQL injection proof
            if data_type == self.STRING:
                 value = self._sql_injection_proof(value)
        return value

    def _get_sql_value(self, value, data_type):
        """
        Get sql value from the given value
        :param value: (dict|string) Value for which sql condition is to be generated
        :param data_type: (string) Data type of the values provided
        :return: (string) sql value to used
        """
        value = self._validate_sql_values(value, data_type)
        if isinstance(value, dict):
            try:
                value_type = value['type'].upper()
            except KeyError as e:
                raise KeyError(
                    'Missing key - [{key}] in value dict'.format(key=e.args[0])
                )
            assert value_type in self.DYNAMIC_VALUE_TYPES, 'Invalid dynamic value type'
            function = getattr(self, self.DYNAMIC_VALUE_MAPPING.get(value_type))
            sql_value = function(value, data_type)
        else:
            # Make value sql proof. For ex: if value is string or data convert it to '<value>'
            (sql_value,) = self._convert_values([value], data_type)
        return sql_value

    def _get_dynamic_date_validated_data(self, value):
        """
        Validate dynamic date data
        :param value: (dict) Value to be validated
        :return: Validated data
        """
        try:
            operator = value['operator'].lower()
            offset = value['offset']
            unit = value['unit']
        except KeyError as e:
            if 'operator' not in value and 'offset' not in value and 'unit' not in value:
                return {'use_now_only': True}
            else:
                raise KeyError(
                    'Missing key - [{key}] in dynamic date value dict'.format(key=e.args[0])
                )
        else:
            if not value['offset']:
                return {'use_now_only': True}
            else:
                if not value['unit'] or not value['operator']:
                    raise ValueError(
                        'Value for unit and operator is required when offset is given in dynamic date'
                    )
        return {'use_now_only': False, 'operator': operator, 'offset': offset, 'unit': unit}

    def _generate_dynamic_date(self, value, data_type):
        """
        Generate dynamic date sql condition
        :param value: (dict) Value for which dynamic date has to be generated
        :param data_type: (string) Data type of the field for which dynamic date is used
        :return: sql value for dynamic date
        """
        validated_data = self._get_dynamic_date_validated_data(value)
        if validated_data.get('use_now_only'):
            return 'NOW()'
        else:
            sql_operator = getattr(self.DYNAMIC_DATE_OPERATORS, validated_data.get('operator'))
            unit = self._sql_injection_proof(validated_data.get('unit')).upper()
            offset = validated_data.get('offset')
            try:
                offset = int(offset)
            except ValueError:
                raise ValueError(
                    'Invalid value for offset - [{key}]'.format(key=offset)
                )
            assert unit in self.DYNAMIC_DATE_UNITS, 'Unsupported dynamic date units'
            return '{date_operator}(NOW(), INTERVAL {offset} {unit})'.format(
                date_operator=sql_operator,
                offset=offset,
                unit=unit,
            )

    def _generate_variable_template(self, value, data_type):
        """
        Generate variable template sql condition
        :param value: (dict) Value for which variable template keyword needs to be generated
        :param data_type: (String) Data type of the field for which variable template is used
        :return: sql value for variable template keyword
        """
        variable_template_id = value.get('variable_template_id')

        # Raise error if variable_template_id key is missing
        if not variable_template_id:
            raise ValueError('Missing key - [variable_template_id]')

        template_data = self.variable_templates[variable_template_id]
        variable_template_keyword = template_data[self.VARIABLE_TEMPLATE_KEYWORD]
        # Check if data type of field is equal to the return type of variable template
        assert template_data[self.VARIABLE_TEMPLATE_RETURN_TYPE] == data_type,\
            'Data type of field does not match return type of {template} variable template'.format(
                template=variable_template_keyword
            )
        (sql_value,) = self._convert_values(['{{{keyword}}}'.format(keyword=variable_template_keyword)], data_type)
        return sql_value

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
            inner_condition = list(element.keys())[0]
            function = getattr(self, self.WHERE_CONDITION_MAPPING.get(inner_condition))
            # Call the function mapped to it.
            result = function(element.get(inner_condition))
            # Append the result to the sql.
            if not sql and condition in [self.AND_CONDITION, self.OR_CONDITION]:
                sql_result = '({result})'.format(result=result)
            else:
                sql_result = ' {condition} ({result})'.format(condition=condition, result=result)
            sql.extend(sql_result.encode('utf8'))
        return u'({sql})'.format(sql=sql.decode('utf8'))

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

    def _sql_injection_proof(self, value):
        """
        Escapes strings to avoid SQL injection attacks
        :param value: (string|unicode) string that needs to be escaped
        :return: (string|unicode) escaped string
        """
        return MySQLdb.escape_string(value).decode('utf8')

    def extract_key_from_nested_dict(self, target_dict, key):
        """
        Traverse the dictionary recursively and return the value with specified key

        :type target_dict: Dict
        :type key: str
        """
        assert isinstance(target_dict, dict)
        assert isinstance(key, str) and key

        for k, v in target_dict.items():
            if k == key:
                yield v
            elif isinstance(v, dict):
                for item in self.extract_key_from_nested_dict(v, key):
                    yield item
