{% test unique_combination_of_columns(model, combination_of_columns, quote_columns=false) %}
  {{ return(adapter.dispatch('test_unique_combination_of_columns', 'dbt_utils')(model, combination_of_columns, quote_columns)) }}
{% endtest %}

{% macro default__test_unique_combination_of_columns(model, combination_of_columns, quote_columns=false) %}

    {% if not quote_columns %}
        {%- set column_list=combination_of_columns %}
    {% elif quote_columns %}
        {%- set column_list=[] %}
        {% for column in combination_of_columns -%}
            {% set column_list = column_list.append( adapter.quote(column) ) %}
        {%- endfor %}
    {% else %}
        {{ exceptions.raise_compiler_error(
            "`quote_columns` argument for unique_combination_of_columns test must be one of [True, False] Got: '" ~ quote ~"'.'"
        ) }}
    {% endif %}

    {%- set columns_csv=column_list | join(', ') %}
    {% set filter = add_days_filter(model, timestamp_column=timestamp_column) %}

    {% if filter.row_condition is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where {{ filter.row_condition }}
        ),
        validation_errors as (
            select
                {{ columns_csv }}
            from filtered_data
            group by {{ columns_csv }}
            having count(*) > 1
        )
    {% else %}
        with validation_errors as (
            select
                {{ columns_csv }}
            from {{ model }}
            group by {{ columns_csv }}
            having count(*) > 1
        )
    {% endif %}

    select *
    from validation_errors


{% endmacro %}
