-- This macro generates the schema name for a dbt model based on the provided custom schema name or defaults to the default schema if none is provided. It is used to ensure that models are built in the correct schema as defined in the dbt project configuration.
-- custom_schema_name is none  → no +schema in dbt_project.yml → use default (dev)
-- custom_schema_name is not none → +schema defined in dbt_project.yml → use that
-- overrides that default concatenation behaviour of dbt which typically combines the default schema with the model name, allowing for more flexible schema naming conventions. This is particularly useful in scenarios where you want to organize models into specific schemas without relying on the default naming patterns.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}