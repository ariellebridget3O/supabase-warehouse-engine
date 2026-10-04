select jsonb_build_object(
         's', sum(u.value)::text,
         'c', count(u.value)
       ) as partial
from public.facts_blocks b
cross join lateral unpack_block(b) u
where b.dataset = ($1->>'dataset')
  and ($1->>'day_from' is null or b.day >= ($1->>'day_from')::date)
  and ($1->>'day_to'   is null or b.day <  ($1->>'day_to')::date)