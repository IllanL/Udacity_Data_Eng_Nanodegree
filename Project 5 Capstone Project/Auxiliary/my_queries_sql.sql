DROP TABLE IF EXISTS countries_treated_code;
CREATE TABLE countries_treated_code AS 
	SELECT a.*, b.iso_country, b.i94cntyl 
	FROM countries_treated AS a
	JOIN country_codes AS b
	ON a.Country_Code = b.CountryCode;

DROP TABLE IF EXISTS airports_treated_code;
CREATE TABLE airports_treated_code AS 
	SELECT a.*, b.CountryCode, b.i94cntyl
	FROM airports_treated AS a
	JOIN country_codes AS b
	ON a.iso_country = b.iso_country;

DROP TABLE IF EXISTS immigration_sample_treated_code;
CREATE TABLE immigration_sample_treated_code AS 
	SELECT a.*,
		b.CountryCode AS cit_CountryCode, 
		b.iso_country AS cit_iso_country,
		c.CountryCode AS res_CountryCode, 
		c.iso_country AS res_iso_country
	FROM immigration_treated AS a
	JOIN country_codes AS b
	ON a.i94cit = b.i94cntyl
	JOIN country_codes AS c
	ON a.i94res = c.i94cntyl;
 
DROP TABLE IF EXISTS demographics_treated_grouped;
CREATE TABLE demographics_treated_grouped AS 
SELECT State, State_Code,
    SUM(Median_Age)/SUM(Total_Population) AS Median_Age,
    SUM(Male_Population) AS Male_Population,
    SUM(Female_Population) AS Female_Population,
    SUM(Total_Population) AS Total_Population,
    SUM(Foreignborn) AS Foreignborn,
    SUM(Average_Household_Size)/SUM(Total_Population) AS Average_Household_Size,
	SUM(Foreignborn)/SUM(Total_Population) AS percent_foreignborn
FROM demographics_treated
GROUP BY State_Code;

DROP TABLE IF EXISTS immigration_facts_table;
CREATE TABLE immigration_facts_table AS 
SELECT a.cicid,
    a.biryear,
    a.gender,
    a.i94yr,
    a.i94mon,
    a.i94cit,
	a.cit_CountryCode,
	a.cit_iso_country,
    a.i94res,
    a.res_CountryCode,
    a.res_iso_country,
    a.i94port,
	d.CountryCode AS CountryAirport,
    a.arrdate,
    a.i94mode,
    a.i94addr,
    c.percent_foreignborn,
    a.depdate,
    a.i94bir,
    a.i94visa,
    a.dtadfile,
    a.dtaddto,
    a.admnum,
    a.fltno,
    a.visatype,
	b.avg_GDP_capita AS res_avg_GDP_capita,
	b.percent_change AS res_GDPcapita_annual_change
FROM immigration_sample_treated_code AS a
LEFT JOIN countries_treated AS b
ON a.res_CountryCode = b.Country_Code
LEFT JOIN demographics_treated_grouped AS c
ON a.i94addr = c.State_Code
LEFT JOIN airports_treated_code AS d
ON a.i94port = d.iata_code;