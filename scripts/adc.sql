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


CREATE TABLE public."Dim_Area"
(
    id serial,
    province_id integer NOT NULL,
    province_name character varying NOT NULL,
    city_id integer NOT NULL,
    city_name character varying NOT NULL
)


insert into "Dim_Date" SELECT
	datum AS date_str,
	EXTRACT(YEAR FROM datum) AS YEAR,
	EXTRACT(MONTH FROM datum) AS MONTH,
	-- Localized month name
	to_char(datum, 'TMMonth') AS MonthName,
	EXTRACT(DAY FROM datum) AS DAY,
	EXTRACT(doy FROM datum) AS DayOfYear,
	-- Localized weekday
	to_char(datum, 'TMDay') AS WeekdayName,
	-- ISO calendar week
	EXTRACT(week FROM datum) AS CalendarWeek,
	to_char(datum, 'dd. mm. yyyy') AS FormattedDate,
	'Q' || to_char(datum, 'Q') AS Quartal,
	to_char(datum, 'yyyy/"Q"Q') AS YearQuartal,
	to_char(datum, 'yyyy/mm') AS YearMonth,
	-- ISO calendar year and week
	to_char(datum, 'iyyy/IW') AS YearCalendarWeek,
	-- Weekend
	CASE WHEN EXTRACT(isodow FROM datum) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS Weekend,
	generate_series(1, 3653) as id,
	-- ISO start and end of the week of this date
	datum + (1 - EXTRACT(isodow FROM datum))::INTEGER AS CWStart,
	datum + (7 - EXTRACT(isodow FROM datum))::INTEGER AS CWEnd,
	-- Start and end of the month of this date
	datum + (1 - EXTRACT(DAY FROM datum))::INTEGER AS MonthStart,
	(datum + (1 - EXTRACT(DAY FROM datum))::INTEGER + '1 month'::INTERVAL)::DATE - '1 day'::INTERVAL AS MonthEnd
FROM (
	-- There are 3 leap years in this range, so calculate 365 * 10 + 3 records
	SELECT '2014-01-01'::DATE + SEQUENCE.DAY AS datum
	FROM generate_series(0,3652) AS SEQUENCE(DAY)
	GROUP BY SEQUENCE.DAY
     ) DQ

-----Dim_Date
SELECT
	datum AS DATE,
	EXTRACT(YEAR FROM datum) AS YEAR,
	EXTRACT(MONTH FROM datum) AS MONTH,
	-- Localized month name
	to_char(datum, 'TMMonth') AS MonthName,
	EXTRACT(DAY FROM datum) AS DAY,
	EXTRACT(doy FROM datum) AS DayOfYear,
	-- Localized weekday
	to_char(datum, 'TMDay') AS WeekdayName,
	-- ISO calendar week
	EXTRACT(week FROM datum) AS CalendarWeek,
	to_char(datum, 'dd. mm. yyyy') AS FormattedDate,
	'Q' || to_char(datum, 'Q') AS Quartal,
	to_char(datum, 'yyyy/"Q"Q') AS YearQuartal,
	to_char(datum, 'yyyy/mm') AS YearMonth,
	-- ISO calendar year and week
	to_char(datum, 'iyyy/IW') AS YearCalendarWeek,
	-- Weekend
	CASE WHEN EXTRACT(isodow FROM datum) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS Weekend,
	-- Fixed holidays 
        -- for America
        CASE WHEN to_char(datum, 'MMDD') IN ('0101', '0704', '1225', '1226')
		THEN 'Holiday' ELSE 'No holiday' END
		AS AmericanHoliday,
        -- for Austria
	CASE WHEN to_char(datum, 'MMDD') IN 
		('0101', '0106', '0501', '0815', '1101', '1208', '1225', '1226') 
		THEN 'Holiday' ELSE 'No holiday' END 
		AS AustrianHoliday,
        -- for Canada
        CASE WHEN to_char(datum, 'MMDD') IN ('0101', '0701', '1225', '1226')
		THEN 'Holiday' ELSE 'No holiday' END 
		AS CanadianHoliday,
	-- Some periods of the year, adjust for your organisation and country
	CASE WHEN to_char(datum, 'MMDD') BETWEEN '0701' AND '0831' THEN 'Summer break'
	     WHEN to_char(datum, 'MMDD') BETWEEN '1115' AND '1225' THEN 'Christmas season'
	     WHEN to_char(datum, 'MMDD') > '1225' OR to_char(datum, 'MMDD') <= '0106' THEN 'Winter break'
		ELSE 'Normal' END
		AS Period,
	-- ISO start and end of the week of this date
	datum + (1 - EXTRACT(isodow FROM datum))::INTEGER AS CWStart,
	datum + (7 - EXTRACT(isodow FROM datum))::INTEGER AS CWEnd,
	-- Start and end of the month of this date
	datum + (1 - EXTRACT(DAY FROM datum))::INTEGER AS MonthStart,
	(datum + (1 - EXTRACT(DAY FROM datum))::INTEGER + '1 month'::INTERVAL)::DATE - '1 day'::INTERVAL AS MonthEnd
FROM (
	-- There are 3 leap years in this range, so calculate 365 * 10 + 3 records
	SELECT '2000-01-01'::DATE + SEQUENCE.DAY AS datum
	FROM generate_series(0,3652) AS SEQUENCE(DAY)
	GROUP BY SEQUENCE.DAY
     ) DQ
ORDER BY 1



---Dim_Time
-- Table: "Dim_Time"

-- DROP TABLE "Dim_Time";

CREATE TABLE "Dim_Time"
(
  id serial NOT NULL,
  hour integer,
  CONSTRAINT "Dim_Time_pkey" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE "Dim_Time"
  OWNER TO postgres;



--AD Facts Table
-- Table: "AD_Facts_By_Hour"

-- DROP TABLE "AD_Facts_By_Hour";

CREATE TABLE "AD_Facts_By_Hour"
(
  date_id integer NOT NULL,
  time_id integer NOT NULL,
  impressions_start_total bigint,
  impressions_finish_total bigint,
  click bigint,
  ad_slot_id integer,
  ad_card_id integer,
  ad_creative_id integer,
  ad_campaign_id integer, //订单ID
  CONSTRAINT "AD_Facts_By_Hour_pkey" PRIMARY KEY (date_id, time_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE "AD_Facts_By_Hour"
  OWNER TO postgres;

CREATE TABLE "Dim_AD"
(
    id serial,
    ad_campaign_id integer NOT NULL,
    ad_slot_id integer NOT NULL,
    ad_card_id integer NOT NULL,
    ad_creative_id integer NOT NULL,
    CONSTRAINT "Dim_AD_pkey" PRIMARY KEY(id)
)

CREATE TABLE "AD_Facts_By_Day"
(
  date_id integer NOT NULL,
  area_id integer,
  video_id integer,
  os_id integer,
  reqs_total bigint,
  impressions_start_total bigint,
  impressions_finish_total bigint,
  click bigint,
  hit_total bigint, //展示数，即命中数
  ad_slot_id integer,
  ad_card_id integer,
  ad_creative_id integer,
  ad_campaign_id integer, //订单ID
  CONSTRAINT "AD_Facts_By_Day_pkey" PRIMARY KEY (date_id, time_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE "AD_Facts_By_Day"
  OWNER TO postgres;

CREATE TABLE "AD_Facts_By_Video"
(
  date_id integer NOT NULL,
  area_id integer,
  video_id integer,
  os_id integer,
  reqs_total bigint,
  impressions_start_total bigint,
  impressions_finish_total bigint,
  click bigint,
  hit_total bigint, //展示数，即命中数
  ad_slot_id integer,
  ad_card_id integer,
  ad_creative_id integer,
  ad_campaign_id integer, //订单ID
  CONSTRAINT "AD_Facts_By_Day_pkey" PRIMARY KEY (date_id, time_id)
)

CREATE TABLE "Reach_Curve_By_Impressions"
(
    ad_campaign_id integer NOT NULL,
    impression_total integer NOT NULL,
    total integer NOT NULL
)

CREATE TABLE "Reach_Curve_By_Day"
(
    date_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    total integer NOT NULL
)

CREATE TABLE public."Dim_Area"
(
    id serial,
    province_id integer NOT NULL,
    province_name character varying NOT NULL,
    city_id integer NOT NULL,
    city_name character varying NOT NULL
)

CREATE TABLE public."Data_Audit_Statistics"
(
    file_name character varying NOT NULL,
    check_date date NOT NULL,
    check_total integer NOT NULL,
    rf float NOT NULL, -- all rows/errors
)

CREATE TABLE public."Data_Audit_Details"
(
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    name character varying NOT NULL,
    row integer NOT NULL,
    field character varying NOT NULL,
    value character varying NOT NULL,
    error character varying NOT NULL
)

CREATE TABLE "Data_Result_Audit"
(
      date_id character varying NOT NULL,
      time_id character varying NOT NULL,
      reqs_errors double precision NOT NULL,
      code_serves_errors double precision NOT NULL,
      impressions_servers_errors double precision NOT NULL,
      unfilled_impressions_errors double precision NOT NULL,
      click_errors double precision NOT NULL,
      serve_rate double precision NOT NULL,
      reqs_e_errors double precision NOT NULL,
      code_serve_e_errors double precision NOT NULL,
      impressions_e_errors double precision NOT NULL
)
WITH (
      OIDS=FALSE
);

CREATE TABLE public."Data_SLA"
(
    id serial,
    name character varying,
    filename character varying,
    exec_success integer,
    exec_error integer,
    create_time date
)

CREATE TABLE "Reqs_Facts_By_Hour"
(
      date_id integer NOT NULL,
      time_id integer NOT NULL,
      os character varying,
      ad_slot_id integer,
      ad_campaign_id integer,
      total integer,
      CONSTRAINT "Reqs_Facts_By_Hour_pkey" PRIMARY KEY (date_id, time_id)
)
WITH (
      OIDS=FALSE
);

CREATE TABLE "Hit_Facts_By_Hour"
(
      date_id integer NOT NULL,
      time_id integer NOT NULL,
      os character varying,
      ad_card_id integer,
      ad_slot_id integer,
      ad_create_id integer,
      ad_campaign_id integer,
      total integer,
      CONSTRAINT "Hit_Facts_By_Hour_pkey" PRIMARY KEY (date_id, time_id)
)
WITH (
      OIDS=FALSE
);

CREATE TABLE "Hit_Facts_By_Hour2"
(
      date_id integer NOT NULL,
      time_id integer NOT NULL,
      ad_card_id integer,
      ad_slot_id integer,
      ad_create_id integer,
      ad_campaign_id integer,
      total integer,
      CONSTRAINT "Hit_Facts_By_Hour2_pkey" PRIMARY KEY (date_id, time_id)
)
WITH (
      OIDS=FALSE
);
