--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: tag_inverted; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE tag_inverted (
    id integer NOT NULL,
    name text NOT NULL,
    description text,
    pid integer,
    uidlist text,
    create_time timestamp without time zone,
    update_time timestamp without time zone
)
DISTRIBUTE BY HASH (id);


ALTER TABLE public.tag_inverted OWNER TO postgres;

--
-- Name: tag_inverted_update; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE tag_inverted_update (
    id integer NOT NULL,
    tag_id integer NOT NULL,
    name text NOT NULL,
    year integer,
    month integer,
    day integer,
    uidlist text,
    create_time timestamp without time zone
)
DISTRIBUTE BY HASH (id);


ALTER TABLE public.tag_inverted_update OWNER TO postgres;

--
-- Name: tag_parent; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE tag_parent (
    id integer NOT NULL,
    name text NOT NULL,
    description text,
    pid integer,
    create_time timestamp without time zone,
    update_time timestamp without time zone
)
DISTRIBUTE BY HASH (id);


ALTER TABLE public.tag_parent OWNER TO postgres;

--
-- Name: user_index; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE user_index (
    id integer NOT NULL,
    uid text NOT NULL,
    deviceid text,
    cookie text,
    imei text,
    adfa text,
    tagidlist text,
    create_time timestamp without time zone,
    update_time timestamp without time zone
)
DISTRIBUTE BY HASH (id);


ALTER TABLE public.user_index OWNER TO postgres;

--
-- Name: user_index_update; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE user_index_update (
    id integer NOT NULL,
    index_id integer NOT NULL,
    uid text NOT NULL,
    year integer,
    month integer,
    day integer,
    tagidlist text,
    create_time timestamp without time zone
)
DISTRIBUTE BY HASH (id);


ALTER TABLE public.user_index_update OWNER TO postgres;

--
-- Data for Name: tag_inverted; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY tag_inverted (id, name, description, pid, uidlist, create_time, update_time) FROM stdin;
\.


--
-- Data for Name: tag_inverted_update; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY tag_inverted_update (id, tag_id, name, year, month, day, uidlist, create_time) FROM stdin;
\.


--
-- Data for Name: tag_parent; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY tag_parent (id, name, description, pid, create_time, update_time) FROM stdin;
\.


--
-- Data for Name: user_index; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY user_index (id, uid, deviceid, cookie, imei, adfa, tagidlist, create_time, update_time) FROM stdin;
\.


--
-- Data for Name: user_index_update; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY user_index_update (id, index_id, uid, year, month, day, tagidlist, create_time) FROM stdin;
\.


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

