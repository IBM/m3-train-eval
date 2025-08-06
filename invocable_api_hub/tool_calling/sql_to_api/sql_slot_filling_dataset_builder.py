import os
from typing import Callable, Tuple

import sqlglot

from invocable_api_hub.tool_calling.sql_to_api.sql_dataset_builder import (
    SqlDatasetBuilder,
    identify_aggregation_expression,
    identify_transformation_expression,
)
from invocable_api_hub.tool_calling.sql_to_api.utils import create_structured_api_call
from invocable_api_hub.tool_calling.tools.slot_filling_sql_tools import (
    aggregate_data,
    filter_data,
    sort_data,
    retrieve_data,
    transform_data,
    select_unique_values,
    group_data_by,
    concatenate_data
)
from invocable_api_hub.tool_calling.tools.sql_tools import initialize_active_data


class SqlSlotFillingDatasetBuilder(SqlDatasetBuilder):
    def __init__(self, database_name: str, dataset_path: str, cache_location: str = None, source_dataset_name: str = None) -> None:
        super().__init__(database_name, dataset_path, cache_location=cache_location, source_dataset_name = source_dataset_name)

        # These functions can stay as is, they are already applied to generic (list-like) objects. 
        base_fcns = {f.__name__: f for f in [
            aggregate_data, 
            filter_data, 
            sort_data, 
            retrieve_data,
            select_unique_values, 
            transform_data,
            group_data_by, 
            concatenate_data,
            initialize_active_data
        ]}
        wrapped_fcns = self.add_read_from_file_decorator(base_fcns)
        self.available_function_dict = wrapped_fcns
        

    def build(self) -> None:
        pass

    def set_query_specific_api_pool(self, api_sequence: list[dict]) ->  Tuple[dict[str, callable], list[dict]]:
        key_names_and_descriptions = self.set_query_specific_columns_and_descriptions(api_sequence)

        # For slot-filling version this is the same for every data point. 
        return self.available_function_dict, key_names_and_descriptions


    def translate_query_from_sql_tree(self, query: str) -> list:

        if query.count("SELECT") > 1:
            raise Exception("Can't support multiple SELECT statements in single query. ")
        
        glot_ast, query, alias_to_table_dict, join_sequence = self.parse_query_and_get_join_metadata(query)
        simplified_dict = self.simplify_alias_to_table_dict(alias_to_table_dict)
        STARTING_TABLE_VAR = "starting_table_var"

        # Load the starting table
        required_api_calls = []
        db_path = os.path.join(self.database_cache_location, self.database_name + ".sqlite")
        required_api_calls.append(create_structured_api_call(
            initialize_active_data, initialize_active_data.__name__, {
                "condition_sequence": join_sequence, "alias_to_table_dict": simplified_dict, "database_path": db_path},
              STARTING_TABLE_VAR))

        # Handle 'where'
        where_calls = self.process_where_clauses(glot_ast, required_api_calls[-1]['label'])
        required_api_calls.extend(where_calls)

        # Handle 'group by'
        groupby_calls = self._process_groupby(glot_ast, required_api_calls[-1]['label'])
        required_api_calls.extend(groupby_calls)

        # Handle 'order'
        order_calls = self._process_orderby_clause(glot_ast, required_api_calls[-1]['label'])
        required_api_calls.extend(order_calls)

        # Create a get for the select
        parsed_select = self.parse_select_clause(glot_ast)
        get_agg_fcn = self._process_select_and_aggregate(parsed_select, required_api_calls[-1]['label'])
        required_api_calls.extend(get_agg_fcn)

        return required_api_calls    


    def _process_groupby(self, ast: sqlglot.Expression, table_var: str) -> dict:
        if 'group' not in ast.args:
            return []

        expression = ast.args['group'].expressions[0]
        get_groupby_column = self.process_column_object(expression)
        groupby_args = {"data_source": f"${table_var}$", "key_name": get_groupby_column}
        groupby_fcn = create_structured_api_call(group_data_by, group_data_by.__name__, groupby_args, 'GROUPED')
        return [groupby_fcn]
    
    def _process_select_and_aggregate(self, parsed_select: dict, table_var: str) -> dict:
        api_calls = []

        limit = parsed_select['limit']
        distinct = parsed_select['distinct']

        for idx, clause in enumerate(parsed_select['clauses']):
            # Handle SUBSTR conditions
            #     if isinstance(eq.this, sqlglot.expressions.Substring):
            #         column_str = str(eq.this.this)
            #         comparison_column = self._reformat_compound_column_name(column_str)
            #         start = eq.this.args['start'].to_py() - 1  # SQL values will be 1-indexed, convert to 0-index
            #         length = eq.this.args['length'].to_py()
            #         operation_args = {"start_index": start, "end_index": start+length}
            #         substring_args = {"data_source": f'${input_df_key}$', "key_name": comparison_column, "operation_type": "substring", "operation_args": operation_args}
            #         api = create_structured_api_call(transform_data, transform_data.__name__, substring_args, 'PROCESSED_DF')
            #         api_calls.append(api)
            #         table_name = '$PROCESSED_DF$'
            col_name = clause[0]
            agg_expr = clause[1]
            select_args = {"data_source": f"${table_var}$", "key_name": col_name, "distinct": distinct, "limit": limit}
            if agg_expr:
                select_args['aggregation_type'] = agg_expr
                get_agg_fcn = create_structured_api_call(aggregate_data, aggregate_data.__name__, select_args, agg_expr.upper())
                api_calls.append(get_agg_fcn)
            else:
                api_calls.append(create_structured_api_call(retrieve_data, retrieve_data.__name__, select_args, 'SELECT_COL_'+str(idx)))

        return api_calls

    def process_where_clauses(self, ast: sqlglot.Expression, input_df_key: str) -> list[Callable]:
        if 'where' not in ast.args:
            return []
        
        where = ast.args['where']
        parsed_where = self.parse_where_clause(where)
        all_where_apis = [] 
        i = 0
        for path in parsed_where:  # Will only be > 1 if there is an OR. 
            where_apis = []
            table_name = f'${input_df_key}$'
            for clause in path:
                comparison_column = clause[0]
                value = clause[1]
                comparison_operator = clause[2]
                is_transform = clause[3]
                if is_transform:
                    operation_args = {'start_index': value[0], 'end_index': value[1]}
                    transform_args = {"data_source": table_name, 
                                    "key_name": comparison_column, 
                                    "operation_type": comparison_operator,
                                    "operation_args": operation_args}
                    output_df = 'TRANSFORMED_DF_'+str(i)
                    api = create_structured_api_call(transform_data, transform_data.__name__, transform_args, output_df)
                else:
                    filter_args = {"data_source": table_name, "key_name": comparison_column, "value": value, "condition": comparison_operator}
                    output_df = 'FILTERED_DF_'+str(i)
                    api = create_structured_api_call(filter_data, filter_data.__name__, filter_args, output_df)
                i += 1
                table_name = '$' + output_df + '$'
                where_apis.append(api)
            all_where_apis.append(where_apis)

        if len(all_where_apis) > 1:
            assert len(all_where_apis) == 2  # Only support a single OR for now. 
            output_table_var_1 = '$' + all_where_apis[0][-1]['label'] + '$'
            output_table_var_2 = '$' + all_where_apis[1][-1]['label'] + '$'
            concat_args = {"data_source_1": output_table_var_1, "data_source_2": output_table_var_2}
            api = create_structured_api_call(concatenate_data, concatenate_data.__name__, concat_args, "CONCAT_DATA")
            return all_where_apis[0] + all_where_apis[1] + [api]
        else:
            return all_where_apis[0]


    def _process_orderby_clause(self, ast: sqlglot.Expression, input_df_key: str) -> list[Callable]:
        if 'order' not in ast.args:
            return []
        
        api_calls = []
        orderby = ast.args['order'].expressions[0]
        assert len(ast.args['order'].expressions) == 1, "Need to implement multiple 'order by' clauses"
        ascending = not bool(orderby.args['desc'])  # return results in descending order
        agg = identify_aggregation_expression(orderby.args['this'])
        transform, transform_args = identify_transformation_expression(orderby.args['this'])
        if agg:  # We need an aggregation before we orderby
            orderby_column = self.process_column_object(orderby.args['this'].this)
            get_agg_args = {"data_source": f"${input_df_key}$", "key_name": orderby_column, "aggregation_type": agg, "distinct": False, "limit": -1}
            get_agg_fcn = create_structured_api_call(aggregate_data, aggregate_data.__name__, get_agg_args, agg.upper())
            api_calls.append(get_agg_fcn)
            table_name = f"${agg.upper()}$"
        elif transform:
            orderby_column = self.process_column_object(orderby.args['this'].this)
            get_trans_args = {"data_source": f"${input_df_key}$", "key_name": orderby_column, "operation_type": transform, "operation_args": transform_args}
            get_trans_fcn = create_structured_api_call(transform_data, transform_data.__name__, get_trans_args, transform.upper())
            api_calls.append(get_trans_fcn)
            table_name = f"${transform.upper()}$"
        else:
            orderby_column = self.process_column_object(orderby.this)
            table_name = f'${input_df_key}$'

        orderby_args = {"data_source": table_name, 'key_name': orderby_column, 'ascending': ascending}
        api = create_structured_api_call(sort_data, sort_data.__name__, orderby_args, 'SORTED_DF')
        api_calls.append(api)
        return api_calls
