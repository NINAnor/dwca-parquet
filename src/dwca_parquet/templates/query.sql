select *, coalesce({% if 'footprintWKT' in columns %}ST_GeomFromText(footprintWKT), {% endif %}ST_POINT(decimalLongitude, decimalLatitude)) as geom
from read_csv('{{core.path}}') as {{ core }}
{% for ext in extensions -%}
    join read_csv('{{ext.path}}') as {{ ext }} on {{ ext }}.{{ ext.id }}={{ core }}.{{ core.id }}
{% endfor %}
