select jsonb_build_object(
         's', sum(t.amount)::text,
         'c', count(t.amount)
       ) as partial
from public.wh_probe_agg t
where ($1->>'id_min' is null or t.id >= ($1->>'id_min')::int8)
  and ($1->>'id_max' is null or t.id <  ($1->>'id_max')::int8)