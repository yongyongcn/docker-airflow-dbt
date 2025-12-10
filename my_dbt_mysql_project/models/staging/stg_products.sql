 
SELECT
  *
from {{ source('seed_test', 'products') }}