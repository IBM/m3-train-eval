import inspect
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from copy import deepcopy
from typing import Callable, Tuple, Union

import sqlglot
import sqlglot.expressions

from invocable_api_hub.tool_calling.sql_to_api.bird_database_loader import BirdDatabaseLoader
from invocable_api_hub.tool_calling.sql_to_api.sql_query_components import (
    make_query_safe,
)
from invocable_api_hub.tool_calling.tools.docstring_parsing_utils import translate_data_type
from invocable_api_hub.tool_calling.tools.file_io_wrappers import (
    load_csv_as_dataframe,
    load_from_csv,
    save_as_csv,
    load_multiple_csvs_as_dataframes,
)
from invocable_api_hub.tool_calling.tools.sql_tools import initialize_active_data

where_dict = {
    sqlglot.expressions.Is: "equal_to",
    sqlglot.expressions.EQ: "equal_to",
    sqlglot.expressions.Like: "like",
    sqlglot.expressions.In: "in",
    sqlglot.expressions.GTE: "greater_than_equal_to",
    sqlglot.expressions.GT: "greater_than", 
    sqlglot.expressions.LTE: "less_than_equal_to", 
    sqlglot.expressions.LT: "less_than",
    sqlglot.expressions.Between: "between"
}
single_where_clause_types = [sqlglot.expressions.Is, sqlglot.expressions.EQ, sqlglot.expressions.Like, sqlglot.expressions.In,
                            sqlglot.expressions.GTE, sqlglot.expressions.GT, sqlglot.expressions.LTE, sqlglot.expressions.LT, 
                            sqlglot.expressions.Between]
transform_dict = {
    sqlglot.expressions.Substring: "substring",
    sqlglot.expressions.StrToTime: "str2time",
    sqlglot.expressions.StrToDate: "str2date", 
    sqlglot.expressions.Abs: "abs"
}


def get_tables_and_aliases(tree: sqlglot.Expression, loader: BirdDatabaseLoader):
    # Get aliases
    alias_to_table_dict = {}
    tables_dict = {}
    tables = defaultdict(list)
    for t in tree.find_all(sqlglot.exp.Table):
        table_name = str(t.this)
        table_alias = t.alias
        tables[table_name].append(table_alias)
    
    tables_metadata = loader.get_table_column_descriptions()
    for table_name in tables.keys():
        count = 0
        for table_alias in tables[table_name]:
            table_data = loader.get_table_as_dataframe(table_name, use_original_not_cache=True)
            modified_table_name = table_name
            if table_alias != '':
                if count > 0:
                    modified_table_name += "_" + str(count)
                original_columns = list(table_data.columns)
                table_data = table_data.add_prefix(modified_table_name+"_")
                modified_columns = list(table_data.columns)
            else:
                original_columns = modified_columns = list(table_data.columns)
            column_descriptions = []
            column_dtypes = []
            for column in original_columns:
                description = tables_metadata[table_name][column]['column_description']
                column_descriptions.append(description)
                dtype = str(tables_metadata[table_name][column]['column_dtype'])
                column_dtypes.append(dtype)
            alias_to_table_dict[table_alias] = {
                'original_table_name': table_name, 
                'modified_table_name': modified_table_name,
                'original_column_names': original_columns,
                'modified_column_names': modified_columns,
                'column_descriptions': column_descriptions,
                'column_dtypes': column_dtypes,
            }
            tables_dict[table_alias] = table_data
            count += 1
    return alias_to_table_dict

def get_join_sequences(tree: sqlglot.Expression):
    # Identify columns and join types from the expression
    condition_sequences = []
    for j in tree.find_all(sqlglot.exp.Join):
        join_type = j.kind
        condition = str(j).split("ON")[1].split("=")
        col1 = condition[0].strip()
        col2 = condition[1].strip()
        condition_sequences.append((col1, col2, join_type))
    return condition_sequences



class DatasetBuilder(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def build(self) -> None:
        pass
    

class SqlDatasetBuilder(DatasetBuilder):
    def __init__(self, database_name: str, dataset_path: str, cache_location: str = None, source_dataset_name: str = None) -> None:
        super().__init__()

        self.database_name = database_name
        self.database_cache_location = cache_location
        if source_dataset_name == "sparc":
            raise Exception("Haven't implemented sparc")
            # self.loader = SparcDatabaseLoader(self.database_name, dataset_path, database_cache_location=cache_location)
        elif source_dataset_name == "cosql":
            raise Exception("Haven't implemented CoSQL")
        elif source_dataset_name == "bird":
                self.loader = BirdDatabaseLoader(self.database_name, dataset_path, database_cache_location=cache_location)
        else:
            raise Exception(f"Source Dataset {source_dataset_name} does not have a loader implemented. ")
        self.loader.load()
        self.available_function_dict = {}
        self.alias_to_table_dict = None

    def build(self) -> None:
        super().build()

    def set_query_specific_columns_and_descriptions(self, api_sequence: list[dict]) -> list[dict]:
        
        assert api_sequence[0]['name'] == 'initialize_active_data', "Sequence must start with 'initialize_active_data'"
        initial_args = api_sequence[0]['arguments']
        join_sequence = initial_args['condition_sequence']
        simplified_dict = initial_args['alias_to_table_dict']
        current_table = initialize_active_data(join_sequence, simplified_dict, self.loader.database_path)
        current_table_keys = list(current_table.keys())

        # Invert the alias and table names to search data loader
        table_to_alias_dict = defaultdict(list)
        for k in simplified_dict.keys():
            table_to_alias_dict[simplified_dict[k]['original_table_name']].append(k)

        # Construct the list of query-specific names and their descriptions
        query_specific_columns_and_descriptions = []
        for table in table_to_alias_dict.keys():
            metadata = self.loader.table_descriptions[table]
            aliases = table_to_alias_dict[table]
            if aliases is None:  # Can't iterate over None
                aliases = ['']
            for alias in aliases:
                for m in metadata:
                    col_name = m['column_name']
                    col_desc = m['column_description']
                    col_dtype = m['column_dtype']
                    col_dtype, _ = translate_data_type(str(col_dtype))
                    modified_col_name = simplified_dict[alias]['modified_table_name'] + "_" + col_name if alias != '' else col_name
                    if modified_col_name in current_table_keys:
                        query_specific_columns_and_descriptions.append({'key_name': modified_col_name, 'description': col_desc, 'dtype': col_dtype})
        try:
            assert len(query_specific_columns_and_descriptions) == len(current_table_keys)
        except:
            print("Failed to build name-description list. ")
            print(f"There were {len(query_specific_columns_and_descriptions)} api calls and {len(current_table_keys)} columns")
            raise Exception("Didn't create the right number of getters. ")
        
        return query_specific_columns_and_descriptions

    def set_query_specific_api_pool(self, api_sequence: list[dict], read_from_files=True) -> tuple[dict[str, callable], dict]:
        raise NotImplementedError()
    
    def add_read_from_file_decorator(self, apis: dict) -> dict:
        for api_name, api in apis.items():
            if api_name == 'initialize_active_data':
                apis[api_name] = save_as_csv(deepcopy(api), tempfile_location=self.database_cache_location)
            elif api_name.startswith("get_"):
                signature = inspect.signature(api)
                param_names = [p.name for p in signature.parameters.values()]
                assert 'data' in param_names
                apis[api_name] = load_from_csv(deepcopy(api))
            elif api_name in ['retrieve_data', 'aggregate_data'] or api_name.startswith("compute_"):
                apis[api_name] = load_from_csv(deepcopy(api))
            elif api_name == "concatenate_data":
                apis[api_name] = load_multiple_csvs_as_dataframes(deepcopy(api), tempfile_location=self.database_cache_location)
            else:
                signature = inspect.signature(api)
                for param in signature.parameters.values():
                    if param.name == 'data':
                        apis[api_name] = load_csv_as_dataframe(deepcopy(api), tempfile_location=self.database_cache_location)
                        break
        return apis
    
    def cleanup_temp_files(self, keep_db=False):
        for directory, subdirs, files in os.walk(self.loader.database_cache_location):
            for f in files:
                if ((not keep_db) and f == self.database_name+".sqlite") or (f.startswith("temp_") and f.endswith(".csv")):
                    found_temp_file = os.path.join(self.loader.database_cache_location, f)
                    os.remove(found_temp_file)
                    print("Removed ", found_temp_file)
                

    def simplify_alias_to_table_dict(self, alias_to_table_dict):
        simple_dict = {}
        for k, v in alias_to_table_dict.items():
            simple_dict[k] = {
                'original_table_name': v['original_table_name'], 
                'modified_table_name': v['modified_table_name']
                }
        return simple_dict

    def translate_query_from_sql_tree(self, query: str) -> dict:
        raise NotImplementedError("Subclass must implement 'translate_query_from_sql_tree")

    def list_available_functions(self, fcn_shortlist: dict = None) -> str:
        if fcn_shortlist is None:
            fcn_shortlist = self.available_function_dict

        function_list = ""
        for i, (fcn_name, fcn) in enumerate(fcn_shortlist.items()):
            function_list += '\n'
            function_list += f"FUNCTION {i}: {fcn_name}"
            function_list += '\n' + "DESCRIPTION: " + '\n'
            function_list += fcn.__doc__
            function_list += '\n'
        return function_list
    
    def _reformat_compound_column_name(self, name: str) -> str:
        # Change "table_alias.column_name" to table_name_column_name
        if '.' in name:
            name = name.split('.')
            name = self.alias_to_table_dict[name[0]]['modified_table_name'] + "_" + name[1]        
        return name

    def parse_query_and_get_join_metadata(self, query: str) -> Tuple[sqlglot.Expression, str]:
        """
        Parses the query into an ast and collects the data needed to specify the joins. 
        
        Also returns the alias_to_table_dict lookup table. 
        """
        # Parse the query into an AST
        safe_query = make_query_safe(query)
        glot_ast = sqlglot.parse_one(safe_query)

        alias_to_table_dict = get_tables_and_aliases(glot_ast, self.loader)
        self.alias_to_table_dict = alias_to_table_dict  # Need to save this in self for other parsing methods to access (where clause)
        condition_sequence = get_join_sequences(glot_ast)

        return glot_ast, safe_query, alias_to_table_dict, condition_sequence
    
    def parse_select_clause(self, ast: sqlglot.Expression) -> dict:

        parsed_select = {
            'clauses': [],
            'limit': -1,
            'distinct': False
        }
        if ast.args.get('limit'):
            parsed_select['limit'] = int(str(ast.args['limit']).split()[1])

        parsed_select['distinct'] = False
        if ast.args.get('distinct', False):
            parsed_select['distinct'] = True
        agg = None
        if isinstance(ast.expressions[0], sqlglot.expressions.Distinct):
            pass # TODO: implement this
        for expression in ast.expressions:
            if isinstance(expression, sqlglot.expressions.Column):
                col_name = self.process_column_object(expression)
                parsed_select['clauses'].append((col_name, None))
            else:
                agg = identify_aggregation_expression(expression)
                col_name = None
                if isinstance(expression.this, sqlglot.expressions.Distinct):
                    parsed_select['distinct'] = True
                    col_name = self.process_column_object(expression.this.expressions[0])
                else:
                    col_name = self.process_column_object(expression.this)
                parsed_select['clauses'].append((col_name, agg))

        return parsed_select

    def process_column_object(self, column_expression: Union[sqlglot.expressions.Column, sqlglot.expressions.Star]) -> str:
        if isinstance(column_expression, sqlglot.expressions.Column):
            if column_expression.table == '':
                column_name = str(column_expression.this)
            else:
                column_name = self.alias_to_table_dict[column_expression.table]['modified_table_name'] + '_' + str(column_expression.this)
            return column_name
        elif isinstance(column_expression, sqlglot.expressions.Star):
            return ""
        else:
            raise Exception(f"Column object of type {type(column_expression)} could not be processed. ")

    def _process_single_filter(self, eq: sqlglot.expressions.EQ, input_df_key: str) -> list[Callable]:
        raise NotImplementedError("Need to implement _process_single_filter")

    def _process_where_clause(self, ast: sqlglot.Expression, input_df_key: str) -> list[Callable]:
        if 'where' not in ast.args:
            return []
        
        # TODO: make this recursive to handle arbitrary numbers of filters
        where = ast.args['where']
        n_clauses = len(str(where).split("AND"))
        if n_clauses == 1:
            required_apis = self._process_single_filter(where.this, input_df_key)
            return required_apis
        elif n_clauses == 2:
            required_apis1 = self._process_single_filter(where.this.this, input_df_key)
            required_apis2 = self._process_single_filter(where.this.expression, required_apis1[-1]['label'])
            return required_apis1 + required_apis2
        elif n_clauses == 3:
            required_apis1 = self._process_single_filter(where.this.this.this, input_df_key)
            required_apis2 = self._process_single_filter(where.this.this.expression, required_apis1[-1]['label'])
            required_apis3 = self._process_single_filter(where.this.expression, required_apis2[-1]['label'])
            return required_apis1 + required_apis2 + required_apis3
        else:
            raise Exception(F"TOO MANY WHERE CONDITIONS: {n_clauses}")

    def parse_transform(self, transform_clause):
        col_name = self.process_column_object(transform_clause.this)
        if isinstance(transform_clause, sqlglot.expressions.Substring):
            start = int(transform_clause.args['start'].this) - 1  # SQL values will be 1-indexed, convert to 0-index
            end = start + int(transform_clause.args['length'].this)
            value = (start, end)
            parsed_transform = (col_name, value, transform_dict[type(transform_clause)], True)
        elif isinstance(transform_clause, sqlglot.expressions.StrToTime):
            pass
        else:
            raise Exception(f"Can't parse transform of type {type(transform_clause)}")
        return parsed_transform

    def parse_single_where_clause(self, clause):
        parsed_clauses = []
        if type(clause.this) in transform_dict.keys():
            transform_clause = self.parse_transform(clause.this)
            parsed_clauses.append(transform_clause)
            col_name = self.process_column_object(clause.this.this)
        elif isinstance(clause.this, sqlglot.expressions.Column):
            col_name = self.process_column_object(clause.this)
        else:
            raise Exception(f"Can't parse where clause nested with {type(clause.this)}")
        
        clause_type = where_dict[type(clause)]
        if clause_type == "between":
            # Between is implemented as two separate filters
            parsed_clauses.append((col_name, clause.args['low'].this, "greater_than", False))
            parsed_clauses.append((col_name, clause.args['high'].this, "less_than", False))
        else:
            value = clause.expression.this
            if isinstance(value, sqlglot.expressions.Identifier):
                value = value.this
            if clause.expression.is_number:
                value = float(value)
            parsed_clauses.append((col_name, value, clause_type, False))
        return parsed_clauses

    @staticmethod
    def prune_where_tree(expr: sqlglot.Expression) -> bool:
        if isinstance(expr.parent, sqlglot.expressions.Paren):
            return True
        if type(expr) in single_where_clause_types:
            return True
        return False
        

    def parse_where_clause(self, ast: sqlglot.Expression) -> dict:

        # Clauses will be a list of lists, where the top-level lists are connected by ORs, and the inner level lists by ANDs
        parsed_where = [[]]

        for node in ast.walk(bfs=False, prune=self.prune_where_tree):
            if isinstance(node, sqlglot.expressions.Or):
                left_side = self.parse_where_clause(node.this)
                right_side = self.parse_where_clause(node.expression)
                parsed_where.extend(left_side)
                parsed_where.extend(right_side)
                break  # Can only handle a single OR for now
            elif isinstance(node, sqlglot.expressions.Paren):
                return(self.parse_where_clause(node.this.copy()))
            # Handle single condition
            elif type(node) in single_where_clause_types:
                parsed_clause = self.parse_single_where_clause(node)
                parsed_where[-1].extend(parsed_clause)
            elif type(node) in [sqlglot.expressions.And, sqlglot.expressions.Where]:
                continue
            else:
                raise Exception(f"Unknown where operator: {type(node)}")
        if parsed_where[0] == []:
            parsed_where = parsed_where[1:]
        return parsed_where



def identify_aggregation_expression(expression: sqlglot.Expression) -> Union[str, None]:
    agg = None
    if isinstance(expression, sqlglot.expressions.Count):
        agg = "count"
    elif isinstance(expression, sqlglot.expressions.Sum):
        agg = "sum"
    elif isinstance(expression, sqlglot.expressions.Min):
        agg = "min"
    elif isinstance(expression, sqlglot.expressions.ArgMin):
        agg = "argmin"
    elif isinstance(expression, sqlglot.expressions.Max):
        agg = "max"
    elif isinstance(expression, sqlglot.expressions.ArgMax):
        agg = "argmax"
    elif isinstance(expression, sqlglot.expressions.Avg):
        agg = "mean"
    elif isinstance(expression, sqlglot.expressions.Stddev):
        agg = "std"
    return agg

def identify_transformation_expression(expression: sqlglot.Expression) -> Union[str, None]:
    transform = None
    transform_args = {}
    if isinstance(expression, sqlglot.expressions.Abs):
        transform = "abs"
        transform_args = {}
    elif isinstance(expression, sqlglot.expressions.Substring):
        transform = "substring"
        transform_args = {}  # TODO: implement this
    return transform, transform_args
