{% macro publish_test_failures() %}

  {%- if execute -%}
    {%- set stage_name = 'DBT_TEST_FAILURES_' ~ modules.datetime.datetime.now().strftime('%Y%m%d_%H%M%S') -%}
    {%- set fully_qualified_stage_name = target.database ~ '.' ~ target.schema ~ '.' ~ stage_name -%}

    {%- set failed_tests = [] -%}
    {%- for result in results -%}
      {%- if result.node.resource_type == 'test' -%}
        {%- if result.status == 'error' or result.status == 'fail' -%}
          {%- if result.node.config.get('store_failures', False) == True or (should_store_failures() and result.node.config.get('store_failures', False) != False) -%}
            {%- set metaplane_meta = result.node.meta.get("metaplane", { 'publish_failures': false }) -%}
            {%- if metaplane_meta.get('publish_failures') == True -%}
              {%- do failed_tests.append({
                'name': result.node.name,
                'unique_id': result.node.unique_id,
                'failures_query': 'select * from ' ~ result.node.relation_name
              }) -%}
            {%- endif -%}
          {%- else -%}
            {{ log("Not staging failures for test " ~ result.node.name ~ " (" ~ result.node.unique_id ~ ") because it is not configured to store failures", info=True) }}
          {%- endif -%}
        {%- endif -%}
      {%- endif -%}
    {%- endfor -%}

    {%- if failed_tests|length > 0 -%}
      {% set stage_and_copy_query %}
        begin;

        use database {{ target.database }};
        use schema {{ target.schema }};

        create or replace file format {{ target.schema }}.csv_format
          type = 'CSV'
          field_delimiter = ','
          record_delimiter = '\n'
          skip_header = 1
          field_optionally_enclosed_by = '"'
          compression = 'NONE';

        create or replace stage {{ fully_qualified_stage_name }}
          file_format = {{ target.schema }}.csv_format
          encryption = (type = 'SNOWFLAKE_SSE');

        grant read,write on stage {{ fully_qualified_stage_name }}
        to role {{ target.role }};

        {% for test in failed_tests %}
          copy into @{{ fully_qualified_stage_name }}/{{ test.name }}.csv
          from ({{ test.failures_query }})
          file_format = (
            type = csv
            field_delimiter = ','
            record_delimiter = '\n'
            field_optionally_enclosed_by = '"'
            compression = 'NONE'
          )
          header = true
          overwrite = true
          single = true;
        {% endfor %}

        commit;
      {% endset %}

      {% do run_query(stage_and_copy_query) %}

      {%- set failure_urls = {} -%}
      {% for test in failed_tests %}
        {% set presigned_url_query %}
          select get_presigned_url(
            '@{{ fully_qualified_stage_name }}',
            '{{ test.name }}.csv',
            43200
          );
        {% endset %}

        {% set presigned_url = run_query(presigned_url_query)[0][0] %}

        {% do failure_urls.update({
          test.unique_id: {
            'results_url': presigned_url
          }
        }) %}

        {{ log("", info=True) }}
        {{ log("Test failures for " ~ test.name ~ " (" ~ test.unique_id ~ "):", info=True) }}
        {{ log("Download URL (valid for 12 hours): " ~ presigned_url, info=True) }}
      {% endfor %}

      {{ log("", info=True) }}
      {{ log("Stage name: @" ~ fully_qualified_stage_name, info=True) }}
      {{ log("Number of failure files: " ~ failed_tests|length, info=True) }}

      {% do metaplane_utils.record_test_failures(failure_urls) %}
    {%- endif -%}
  {%- endif -%}
{% endmacro %}