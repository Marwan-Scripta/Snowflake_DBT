-- This macro generates a database name based on the provided custom name or the target database.
-- if the target environment is 'test', it appends '_TEST' to the database name if it does not already end with that suffix.
{% macro generate_database_name(custom_database_name, node) %}
    {% set db = custom_database_name if custom_database_name is not none else target.database %}

    {% if target.name == "test" and not db.endswith("_TEST") %}
        {{ return(db ~ "_TEST") }}
    {% else %}
        {{ return(db) }}
    {% endif %}
{% endmacro %}