select to_jsonb(t) as row_json,
       count(*) over () as _pre_trim
from public.wh_probe_agg t
order by t.amount desc nulls last, t.id asc
limit $2