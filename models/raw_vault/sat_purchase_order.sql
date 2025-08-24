select
    Purchase_order_hky,
    LDTS,
    DELETE_FLAG,
    ORDERDATE,
    SUBTOTAL,
    TOTALDUE
from   {{ source('EEE_TRAINING_DV','sat_purchase_order') }}