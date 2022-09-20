{% set databases_lists = databases_list('APP_FOLLOWUP',
   ['teste','teste2','teste3'])%}

{{ insights('orders_insights', databases_lists) }}
