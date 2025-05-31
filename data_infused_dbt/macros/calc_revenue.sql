{% macro calc_revenue(quantity, price) %}
    {{ quantity }} * {{ price }}
{% endmacro %}
