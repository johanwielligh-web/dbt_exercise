select
    Product_hky,
    LDTS,
    DELETE_FLAG,
    CATEGORY_NAME,
    SUBCATEGORY_NAME,
    LISTPRICE,
    PRODUCT_NAME
from   {{ source('EEE_TRAINING_DV','sat_product') }}