select jsonb_build_object(
         'k', jsonb_build_array(g.region),
         'a', jsonb_build_object('x', g.s::text, 'c', g.c, 'n', g.n)
       ) as row_json,
       count(*) over () as _pre_trim
from (select t.region,
             sum(t.amount) as s,
             count(t.amount) as c,
             count(*)      as n
      from public.wh_probe_agg t
      join public.wh_probe_dim d on t.region = d.region and d.tier = 2
      group by t.region) g
order by g.region asc
limit $2