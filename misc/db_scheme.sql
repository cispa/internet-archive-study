
CREATE TABLE archiveorg_indices (
    id SERIAL,
    date timestamp without time zone,
    actual_date timestamp without time zone,
    url text,
    final_url text,
    status text,
    error text,
    pos integer
);

CREATE TABLE cdx_archive_headers (
    id SERIAL,
    cdx_responses_id integer,
    url_date date,
    start_url character varying(128),
    end_url character varying(1024),
    headers jsonb,
    "timestamp" timestamp without time zone DEFAULT now(),
    content_hash character varying(64) DEFAULT NULL::character varying,
    status_code integer DEFAULT '-1'::integer
);

CREATE TABLE cdx_responses (
    id SERIAL,
    tranco_id integer,
    domain character varying(128),
    "timestamp" timestamp without time zone DEFAULT now(),
    content_hash character varying(64) DEFAULT NULL::character varying,
    status_code integer DEFAULT 200,
    parsed boolean DEFAULT false,
    error text
);

CREATE TABLE dataset (
    rank integer,
    url character varying(256)
);

CREATE TABLE duplicates (
    arch character varying(10),
    url character varying(64),
    date timestamp without time zone,
    max integer
);

CREATE TABLE dynamic_script_inclusions_2022 (
    id integer,
    year integer,
    url text,
    request_url text,
    result jsonb,
    status_code integer,
    request_site text,
    response_status_code integer,
    response_headers jsonb,
    trackers jsonb
);

CREATE TABLE dynamic_script_inclusions_2016 (
    id integer,
    year integer,
    url text,
    request_url text,
    result jsonb,
    status_code integer,
    request_site text,
    response_status_code integer,
    response_headers jsonb,
    trackers jsonb
);

CREATE TABLE historical_data (
    id SERIAL,
    tranco_id integer,
    domain character varying(128),
    start_url character varying(128),
    end_url character varying(2048),
    headers jsonb,
    "timestamp" timestamp without time zone DEFAULT now(),
    content_hash character varying(64) DEFAULT NULL::character varying,
    status_code integer DEFAULT '-1'::integer,
    duration integer
);

CREATE TABLE live_headers (
    id SERIAL,
    tranco_id integer,
    domain character varying(64),
    start_url character varying(64),
    end_url character varying(1024),
    headers jsonb,
    "timestamp" timestamp without time zone DEFAULT now(),
    content_hash character varying(64) DEFAULT NULL::character varying,
    status_code integer DEFAULT '-1'::integer,
    script_info jsonb,
    security_headers jsonb,
    trackers text[],
    duration integer
);

CREATE TABLE random_dataset (
    rank integer,
    url character varying(256)
);

CREATE TABLE responses (
    id SERIAL,
    arch character varying(10),
    date timestamp without time zone,
    url character varying(64),
    status integer,
    headers jsonb,
    final_url text,
    error text,
    runtime double precision,
    content_hash character varying(64),
    valid boolean DEFAULT false,
    valid_html boolean DEFAULT false,
    valid_headers boolean DEFAULT false,
    actual_date timestamp without time zone,
    script_info jsonb,
    security_headers jsonb,
    archived_url character varying(2048) DEFAULT NULL::character varying,
    trackers text[],
    length integer
);

CREATE TABLE responses_for_comparison (
    id SERIAL,
    arch character varying(10),
    date timestamp without time zone,
    url character varying(64),
    status integer,
    headers jsonb,
    final_url text,
    error text,
    runtime double precision,
    content_hash character varying(64),
    valid boolean,
    valid_html boolean,
    valid_headers boolean,
    actual_date timestamp without time zone,
    script_info jsonb,
    security_headers jsonb,
    archived_url character varying(2048),
    trackers text[],
    length integer
);

CREATE TABLE responses_initial (
    id SERIAL,
    arch character varying(256),
    date timestamp without time zone,
    url character varying(64),
    status integer,
    headers jsonb,
    final_url text,
    error text,
    runtime double precision,
    content_hash character varying(64) DEFAULT NULL::character varying,
    valid boolean DEFAULT false,
    valid_html boolean DEFAULT false,
    valid_headers boolean DEFAULT false,
    actual_date timestamp without time zone,
    archived_url character varying(2048)
);


CREATE TABLE responses_neighbors (
    id SERIAL,
    arch character varying(10),
    date timestamp without time zone,
    url character varying(64),
    status integer,
    headers jsonb,
    final_url text,
    error text,
    runtime double precision,
    content_hash character varying(64),
    valid boolean DEFAULT false,
    valid_html boolean DEFAULT false,
    valid_headers boolean DEFAULT false,
    actual_date timestamp without time zone,
    script_info jsonb,
    security_headers jsonb,
    archived_url character varying(2048) DEFAULT NULL::character varying,
    trackers text[],
    length integer,
    pos integer,
    final_date timestamp without time zone,
    datasource jsonb,
    interesting_null text[],
    iframe_info jsonb,
    trackers_iframe text[],
    src_inclusion_info jsonb,
    trackers_src_inclusion text[],
    trackers_easyprivacy text[],
    security_headers_ext jsonb
);

CREATE TABLE web_archive_headers (
    id SERIAL,
    tranco_id integer,
    domain character varying(128),
    start_url character varying(128),
    end_url character varying(1024),
    headers jsonb,
    "timestamp" timestamp without time zone DEFAULT now(),
    content_hash character varying(64) DEFAULT NULL::character varying,
    status_code integer DEFAULT '-1'::integer
);


