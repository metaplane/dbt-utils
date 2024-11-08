{% macro mp_record_test_failures(failure_meta) %}

  {%- if execute -%}

    {% for result in results %}
      {% if result.node.unique_id in failure_meta %}
        {% set meta = failure_meta[result.node.unique_id] %}
        {% do result.adapter_response.update({
          "mp_test_failure_meta": meta
        }) %}
      {% endif %}
    {% endfor %}

  {%- endif -%}

{% endmacro %}