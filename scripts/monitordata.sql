CREATE database monitordata;

CREATE TABLE "Reqs_Fact_By_Hour0"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Reqs_Fact_By_Hour0_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Reqs_Fact_By_Hour1"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Reqs_Fact_By_Hour1_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Reqs_Fact_By_Day0"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Reqs_Fact_By_Day0_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Reqs_Fact_By_Day1"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Reqs_Fact_By_Day1_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Serves_Fact_By_Hour0"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Serves_Fact_By_Hour0_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Serves_Fact_By_Hour1"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Serves_Fact_By_Hour1_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Serves_Fact_By_Day0"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Serves_Fact_By_Day0_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Serves_Fact_By_Day1"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Serves_Fact_By_Day1_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Impressions_Fact_By_Hour0"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Impressions_Fact_By_Hour0_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Impressions_Fact_By_Hour1"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Impressions_Fact_By_Hour1_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Impressions_Fact_By_Day0"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Impressions_Fact_By_Day0_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Impressions_Fact_By_Day1"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Impressions_Fact_By_Day1_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Click_Fact_By_Hour0"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Click_Fact_By_Hour0_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Click_Fact_By_Hour1"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Click_Fact_By_Hour1_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Click_Fact_By_Day0"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Click_Fact_By_Day0_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Click_Fact_By_Day1"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Click_Fact_By_Day1_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Error_Dim"
(
    id serial NOT NULL,
    name character varying NOT NULL,
)

CREATE TABLE "Error_Fact_By_Hour0"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    cat integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Error_Fact_By_Hour0_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Error_Fact_By_Hour1"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_unit_id integer NOT NULL,
    area_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    os_id integer NOT NULL,
    video_id integer NOT NULL,
    cat integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Error_Fact_By_Hour1_pk" PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Reach_Curve_By_Day0"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Reach_Curve_By_Day0_pk" PRIMARY KEY(id)
)

CREATE TABLE "Reach_Curve_By_Day1"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Reach_Curve_By_Day1_pk" PRIMARY KEY(id)
)

CREATE TABLE "Reach_Curve_By_Impressions0"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    impression_total integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    total integer NOT NULL,
    CONSTRAINT "Reach_Curve_By_Impressions0_pk" PRIMARY KEY(id)
)

CREATE TABLE "Reach_Curve_By_Impressions1"
(
    id serial NOT NULL,
    date_id integer NOT NULL,
    time_id integer NOT NULL,
    impression_total integer NOT NULL,
    ad_campaign_id integer NOT NULL,
    total integer NOT NULL
    CONSTRAINT "Reach_Curve_By_Impressions1_pk" PRIMARY KEY(id)
)

CREATE TABLE "Data_Audit_Details"
(
      date_id character varying(10) NOT NULL,
      name character varying NOT NULL,
      field character varying NOT NULL,
      value character varying NOT NULL,
      error character varying NOT NULL,
      o_id character varying(40) NOT NULL,
      CONSTRAINT "Data_Audit_Details_pkey" PRIMARY KEY (name, o_id, date_id)
);

CREATE TABLE "Data_Audit_Statistics"
(
    id serial NOT NULL,
    file_name character varying NOT NULL,
    check_date character varying(10) NOT NULL,
    check_total integer NOT NULL,
    rf double precision NOT NULL,
    CONSTRAINT "Data_Audit_Statistics_pk" PRIMARY KEY (id)
);

CREATE TABLE "Data_Result_Audit"
(
    id serial NOT NULL,
    date_id character varying NOT NULL,
    time_id character varying NOT NULL,
    reqs_errors double precision NOT NULL,
    code_serves_errors double precision NOT NULL,
    impressions_errors double precision NOT NULL,
    unfilled_impressions_errors double precision NOT NULL,
    click_errors double precision NOT NULL,
    serve_rate double precision NOT NULL,
    reqs_e_errors double precision NOT NULL,
    code_serve_e_errors double precision NOT NULL,
    impressions_e_errors double precision NOT NULL,
    CONSTRAINT "Data_Result_Audit_pk" PRIMARY KEY (id)
);

CREATE TABLE "Gene"
(
    id serial NOT NULL,
    uid character varying,
    deviceid character varying,
    cookie character varying,
    imei character varying,
    adfa character varying,
    create_time date,
    update_time date,
    CONSTRAINT "Gene_pk" PRIMARY KEY (id)
)

CREATE TABLE "Dim_Date"
(
    date_id integer NOT NULL,
    year double precision,
    month double precision,
    monthname text,
    day double precision,
    dayofyear double precision,
    weekdayname text,
    calendarweek double precision,
    formatteddate text,
    quartal text,
    yearquartal text,
    yearmonth text,
    yearcalendarweek text,
    weekend text,
    id serial NOT NULL,
    cwstart text,
    cwend text,
    monthstart text,
    monthend text,
    CONSTRAINT "Dim_Date_pk" PRIMARY KEY (date_id)
);

CREATE TABLE "Dim_Time"
(
    id serial NOT NULL,
    hour integer,
    CONSTRAINT "Dim_Time_pk" PRIMARY KEY (id)
)
