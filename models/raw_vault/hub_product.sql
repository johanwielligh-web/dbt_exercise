select
    Product_hky,
    LDTS,
    LLOAD_ID,
    Record_source2,
    Product_bk
from   {{ source('EEE_TRAINING_DV','hub_product') }}