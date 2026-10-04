select jsonb_build_object(
         'k', jsonb_build_array(g.region),
         'a', jsonb_build_object('x', g.s::text, 'c', g.c)
       ) as row_json,
       count(*) over () as _pre_trim
from (select t.region,
             sum(t.amount) as s,
             count(*)      as c
      from public.wh_probe_agg t
      where ($1->>'id_min' is null or t.id >= ($1->>'id_min')::int8)
        and ($1->>'id_max' is null or t.id <  ($1->>'id_max')::int8)
      group by t.region) g
order by g.region asc
limit $2