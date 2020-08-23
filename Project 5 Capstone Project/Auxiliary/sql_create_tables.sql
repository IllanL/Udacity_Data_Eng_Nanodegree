# CREATE SCHEMA 000_DATA_ENG;
# USE 000_DATA_ENG;

CREATE TABLE immigration_treated (
	Imm_id BIGINT PRIMARY KEY,
	cicid BIGINT,
	i94yr SMALLINT,
	i94mon SMALLINT, 
	i94cit INT,
	i94res INT,
	i94port	VARCHAR(5),
	arrdate	DATE,
	i94mode	TINYINT,
	i94addr	VARCHAR(10),
	depdate	DATE,
	i94bir SMALLINT,
	i94visa	SMALLINT,
	counted SMALLINT,
	dtadfile BIGINT,
	entdepa	VARCHAR(10),
	entdepd	VARCHAR(10),	
	matflag	VARCHAR(10),
	biryear	SMALLINT,
	dtaddto BIGINT,	
	gender CHAR(1),
	airline	VARCHAR(10),
	admnum BIGINT,
	fltno VARCHAR(10),
	visatype VARCHAR (10)
);


CREATE INDEX `idx_immigration_treated_i94port`  
ON `000_data_eng`.`immigration_treated` (i94port) 
COMMENT '' ALGORITHM DEFAULT 
LOCK DEFAULT;


CREATE TABLE demographics_treated (
	City VARCHAR(200),	
	State VARCHAR(20),
	Median_Age FLOAT,	
	Male_Population BIGINT,
	Female_Population BIGINT,
	Total_Population BIGINT,
	Foreignborn BIGINT,
	Average_Household_Size FLOAT,
	State_Code CHAR(2),
	Race VARCHAR(50),
	Counted BIGINT
);


CREATE TABLE countries_treated (
	Country_Name VARCHAR(100),
	Country_Code VARCHAR(6),
	avg_GDP_capita BIGINT,
	max_year SMALLINT,
	GDP_capita_max_year	FLOAT,
	min_year SMALLINT,
	GDP_capita_min_year FLOAT,	
	percent_change DOUBLE
);


CREATE TABLE airports_treated (
	ident VARCHAR(20) PRIMARY KEY,
	type VARCHAR(150),
	name VARCHAR(300),
	iso_country	VARCHAR(20),
	iso_region VARCHAR(50),
	municipality VARCHAR(300),
	gps_code VARCHAR(5),
	iata_code VARCHAR(5) FOREIGN KEY,
	local_code VARCHAR(5),
	longitude FLOAT,
	latitude FLOAT,
	FOREIGN KEY (iata_code) REFERENCES immigration_treated(i94port)
);

CREATE TABLE temperatures_treated (
	month TINYINT,
	year INT,
	City VARCHAR(200),
	Country	VARCHAR(200),
	AverageTemperature FLOAT,
	AverageTemperatureUncertainty FLOAT,
	AvgTotal FLOAT
);


CREATE TABLE country_codes (
Country VARCHAR(500),	
Country_norm VARCHAR(500),
iso_country	CHAR(2),
CountryCode	CHAR(3),
CodeNum	INT,
i94cntyl INT
);