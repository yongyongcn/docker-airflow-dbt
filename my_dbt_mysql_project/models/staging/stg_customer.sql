
SELECT
  *
from {{ source('seed_test', 'customer') }}