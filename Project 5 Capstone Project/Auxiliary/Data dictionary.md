**Data dictionary**

Here we leave a Data dictionary explaining the values stored in our file:

**immigration_facts_table:**

    cicid--> CIC's foreign traveler id
    
    biryear--> date of birth of such foreigner
    
    gender--> gender of the foreigner (values as "0" and "X" exist for unknown data)
    
    i94yr--> year of the travel
    
    i94mon--> month of the travel
    
    i94cit--> USA-CIC's traveler's country of citizenship
    
    cit_CountryCode--> Country Code of traveler's country of citizenship
    
    cit_iso_country--> ISO Country Code of traveler's country of citizenship
    
    i94res--> USA-CIC's traveler's country of residence
    
    res_CountryCode--> Country Code of traveler's country of residence
    
    res_iso_country--> ISO Country Code of traveler's country of residence
    
    i94port--> i94 code destination airport
    
    CountryAirport--> Airport's country
    
    arrdate--> Date of arraival
    
    i94mode--> Way the traveler has entered the USA: values include 'Air', 'Sea', 'Land' and 'Not reported'
    
    i94addr--> State code of traveler's residence address
    
    percent_foreignborn--> State's percentage of people foreign born, as calculated aggregatin data from the demographics file (differences to real data may exist)
    
    depdate--> Date of departure for the foreigner
    
    i94bir--> Age of the respondent in years
    
    i94visa--> Type of visa the respondent holds
    
    visatype--> Type of visa hold by traveler
    
    res_avg_GDP_capita--> Average GDP per capita (of last 10 years) of traveler's country of residence
    
    res_GDPcapita_annual_change--> Average annual percent increase of decrease of the GDP per capita of traveler's country of residence (calculated over the last 10 years)

 **airports_treated_code:**

    ident--> Airport's identity code
    
    type--> Kind of airport (small_airport, medium_airport, large_aiport, etc...)
    
    name--> Airport's name
    
    iso_country--> ISO code for airport's country
    
    iso_region--> ISO code for airport's region
    
    municipality--> Airport's city
    
    gps_code--> Airport's GPS code
    
    iata_code--> Airport's code in IATA tables
    
    local_code--> Local designation
    
    longitude--> Airport's longitude
    
    latitude--> Airport's latitude
    
    CountryCode--> Airport's country code
    
    i94cntyl--> I94 airport's code


​    
 **demographics_treated_grouped:**  
​    
    State--> USA's State Name
    
    State Code--> USA's 2-letter code
    
    Median_Age--> State's median age, as per the aggregation of the value from the demographics dataset
    
    Male_Population--> Number of males in state, as per the aggregation of the value from the demographics dataset
    
    Female_Population--> Number of females in state, as per the aggregation of the value from the demographics dataset
    
    Total_Population--> State's total population, as per the aggregation of the value from the demographics dataset
    
    Foreignborn--> Number of people born abroad in the state, as per the aggregation of the value from the demographics dataset
    
    Average_Household_Size--> State's average household size, as per the aggregation of the value from the demographics dataset
    
    Percent_Foreignborn--> Percentage of people born abroad, as per the aggregation of the value from the demographics dataset


​    
**temperatures_treated:**   
​    
    month--> month of record
    
    year--> year of record
    
    City--> City of record
    
    Country--> Country's city of record
    
    AverageTemperature--> Month's average temperature (last 10 years: 2010-2019)
    
    AverageTemperatureUncertainty--> Month's average temperature uncertainty (last 10 years: 2010-2019)
    
    AvgTotal--> City's average temperature for the last 10 years (2010-2019)

**countries_treated:**    
    
    Country Name--> Name of the country
    
    Country Code--> Country 3-digit code
    
    avg_GDP per capita_capita--> Last 10 year's average GDP per capita data
    
    max_year--> year of last available medition
    
    GDP per capita_capita_max_year--> GDP per capita of last year available
    
    min_year--> year of first medition since 2010 available
    
    GDP per capita_capita_min_year--> GDP per capita of that year
    
    %_change--> annual average GDP per capita increase, in percentage points


**country_codes:**

    Country--> Country's name
    
    Country_norm--> Country's name "normalized" (some minor misspellings corrected, plus other corrections)
    
    iso_country--> Country's ISO code
    
    CountryCode--> Country's 3-digit code
    
    CodeNum--> Country's international code number
    
    i94cntyl--> i94 Country's number