CREATE NOTIFICATION INTEGRATION email_int
  TYPE=EMAIL
  ENABLED=TRUE
  ALLOWED_RECIPIENTS=('nkonduri@gmail.com');


CALL SYSTEM$SEND_EMAIL(
    'email_int',
    'nkonduri@gmail.com',
    'Email Alert: PROCESS_DIM_FACILITY has finished.',
    'Processed 30k records'
);