select
    Purchase_order_hky,
    LDTS,
    LLOAD_ID,
    Record_source2,
    Purchase_order_bk
from   {{ source('EEE_TRAINING_DV','hub_purchase_order') }}