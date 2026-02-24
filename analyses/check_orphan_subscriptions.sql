select s.subscription_id
from {{ ref('stg_saas__subscriptions') }} s
left join {{ ref('stg_saas__users') }} u
    on s.user_id = u.user_id
where u.user_id is null