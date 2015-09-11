CREATE TABLE public."Gene"
(
   id serial, 
   uid character varying, 
   deviceid character varying, 
   cookie character varying, 
   imei character varying, 
   adfa character varying, 
   create_time date, 
   update_time date, 
   CONSTRAINT id PRIMARY KEY (id)
) 
WITH (
  OIDS = FALSE
)
;
--ALTER TABLE public."Gene"
--OWNER TO postgres;

CREATE DATABASE adc
  WITH ENCODING='UTF8'
       OWNER=postgres
       CONNECTION LIMIT=-1;

