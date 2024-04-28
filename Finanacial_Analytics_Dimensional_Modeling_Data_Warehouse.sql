-- IMPORT / LOAD RAW DATASETS
-- Staging Dataset - Historical Stock Data
CREATE TABLE staging_historical_stock_data (
	Symbol VARCHAR(20),
	Name VARCHAR(255),
	Last_Sale MONEY,
	Net_Change NUMERIC(10, 5),
	Percent_Change VARCHAR(20),
	Market_Cap NUMERIC(20, 5),
	Country VARCHAR,
	IPO_Year INT,
	Volume INT,
	Sector VARCHAR(255),
	Industry VARCHAR(255)
);
-- DROP TABLE staging_historical_stock_data;
SELECT * FROM staging_historical_stock_data;

-- Staging Dataset - GDP Data
CREATE TABLE staging_GPD_data (
	LOCATION VARCHAR(255),
	Country VARCHAR(255),
	TRANSACT VARCHAR(255),
	Transaction VARCHAR(255),
	MEASURE_Code VARCHAR(255),
	Measure VARCHAR(255),
	TIME INT,
	Year INT,
	Unit_Code VARCHAR(255),
	Unit VARCHAR(255),
	PowerCode_Code INT,
	PowerCode VARCHAR(255),
	Reference_Period_Code INT,
	Reference_Period INT,
	Value NUMERIC(20, 0),
	Flag_Code VARCHAR(255),
	Flags VARCHAR(255)
);
-- Check if the data was successfully imported or loaded.
SELECT * FROM staging_GPD_data;

-- Staging Dataset - UN Comtrade Data
CREATE TABLE staging_UN_Comtrade_data (
	datasetCode NUMERIC(20, 0),
	typeCode VARCHAR(255),
	freqCode VARCHAR(255),
	period INT,
	reporterCode INT,
	reporterDescription VARCHAR(255),
	currency VARCHAR(255),
	importConvFactor NUMERIC(30, 20),
	exportConvFactor NUMERIC(30, 20),
	tradeSystem VARCHAR(255),
	classificationCode VARCHAR(255),
	importValuation VARCHAR(255),
	exportValuation VARCHAR(255),
	importPartnerCountry VARCHAR(255),
	exportPartnerCountry VARCHAR(255),
	importPartner2Country VARCHAR(255),
	exportPartner2Country VARCHAR(255),
	publicationNote TEXT,
	publicationDate TIMESTAMP,
	publicationDateShort VARCHAR(10)
);
-- Check if the data was successfully imported or loaded.
SELECT * FROM staging_UN_Comtrade_data;

-- EXTRACT, TRANSFORM, LOAD (ETL) 
-- Note, all data has been extracted from the downloaded csv files into Postgre.
-- Table: staging_historical_stock_data --
-- Number of records that have any null values.
SELECT COUNT(*) AS null_count 
FROM staging_historical_stock_data
WHERE 
	(
		last_sale IS NULL OR
		net_change IS NULL OR
		percent_change IS NULL OR
		market_cap IS NULL OR
		country IS NULL OR
		ipo_year IS NULL OR
		volume IS NULL OR
		sector IS NULL OR
		industry IS NULL	
	);
-- 3577 out of 7198 records have null values in other columns. 

-- Check the null values for each column
SELECT 
	'symbol' AS column_name,
	COUNT(*) AS null_count
FROM staging_historical_stock_data
WHERE symbol IS NULL
UNION ALL
SELECT
	'name' AS column_name,
	COUNT(*) AS null_count
FROM staging_historical_stock_data
WHERE name IS NULL
UNION ALL
SELECT	
	'last_sale' AS column_name,
	COUNT(*) AS null_count
FROM staging_historical_stock_data
WHERE last_sale IS NULL
UNION ALL
SELECT 
	'net_change' AS column_name,
	COUNT(*) AS null_count
FROM staging_historical_stock_data
WHERE net_change IS NULL
UNION ALL
SELECT 
	'percent_change' AS column_name,
	COUNT(*) AS null_count
FROM staging_historical_stock_data
WHERE percent_change IS NULL
UNION ALL
SELECT 
	'market_cap' AS column_name,
	COUNT(*) AS null_count
FROM staging_historical_stock_data
WHERE market_cap IS NULL
UNION ALL
SELECT 
	'country' AS column_name,
	COUNT(*) AS null_count
FROM staging_historical_stock_data
WHERE country IS NULL
UNION ALL
SELECT 
	'ipo_year' AS column_name,
	COUNT(*) AS null_count
FROM staging_historical_stock_data
WHERE ipo_year IS NULL
UNION ALL
SELECT 
	'volume' AS column_name,
	COUNT(*) AS null_count
FROM staging_historical_stock_data
WHERE volume IS NULL
UNION ALL
SELECT 
	'sector' AS column_name,
	COUNT(*) AS null_count
FROM staging_historical_stock_data
WHERE sector IS NULL
UNION ALL
SELECT 
	'industry' AS column_name,
	COUNT(*) AS null_count
FROM staging_historical_stock_data
WHERE industry IS NULL;
-- Columns with null values include country, market_cap, industry, sector, ipo_year

-- For country, we will replace null_counts with 'Unknown'
-- We are not making any assumptions so that the results are not influenced in a different direction.
UPDATE staging_historical_stock_data
SET country = 'Unknown'
WHERE country IS NULL;
-- For market-cap, replace null values with 0.00000
UPDATE staging_historical_stock_data
SET market_cap = 0.00000
WHERE market_cap IS NULL;
-- For Industry, check if it is the same record with 'Sector' that is null.
-- If not, check the industries and match them with the sectors
SELECT sector, industry
FROM staging_historical_stock_data
WHERE industry IS NULL;
-- All null_counts for 'industry' corresponds with all null_counts for 'sector'.
-- We do not want to make assumptions in this dataset as that might influence the results.
-- Hence, we will replace the null_values with 'Unknown'.
-- Sector
UPDATE staging_historical_stock_data
SET sector = 'Unknown'
WHERE sector IS NULL;
-- Industry
UPDATE staging_historical_stock_data
SET industry = 'Unknown'
WHERE industry IS NULL;
-- For IPO_year, we will replace null values with '9999'.
-- We are not making any assumptions here in order not to influence the results in a different direction. 
UPDATE staging_historical_stock_data
SET ipo_year = 9999
WHERE ipo_year IS NULL;
-- Change the data type for IPO_year
ALTER TABLE staging_historical_stock_data
ALTER COLUMN ipo_year SET DATA TYPE DATE
USING TO_DATE(ipo_year::text || '-01-01', 'YYYY-MM-DD');
--- Change data types ---
-- Remove the % sign on the 'percent_change' column to make it easier for mathematical calculations, if any.
UPDATE staging_historical_stock_data
SET percent_change = REPLACE(percent_change, '%', '');
-- Change the data type for the column
ALTER TABLE staging_historical_stock_data
ALTER COLUMN percent_change TYPE NUMERIC(10, 5)
USING percent_change::numeric(10, 5);
-- To aid calculations and normalization, change data type for 'last_sale'
ALTER TABLE staging_historical_stock_data
ALTER COLUMN last_sale TYPE VARCHAR(255)
USING last_sale::VARCHAR(255);
-- Remove the dollar sign in from the column
UPDATE staging_historical_stock_data
SET last_sale = REPLACE(last_sale, '$', '');
UPDATE staging_historical_stock_data
SET last_sale = REPLACE(last_sale, ',', '');
-- Change it back to numeric type
ALTER TABLE staging_historical_stock_data
ALTER COLUMN last_sale TYPE Numeric(20, 5)
USING last_sale::Numeric(20, 5);


-- Table: staging_UN_Comtrade_data --
-- Columns that do not have any null values.
SELECT COUNT(*) AS null_count 
FROM staging_UN_Comtrade_data
WHERE 
	NOT (
		datasetCode IS NULL OR
		typeCode IS NULL OR
		freqCode IS NULL OR
		period IS NULL OR
		reporterCode IS NULL OR
		reporterDescription IS NULL OR
		currency IS NULL OR
		importConvFactor IS NULL OR
		exportConvFactor IS NULL OR
		tradeSystem IS NULL OR
		classificationCode IS NULL OR
		importValuation IS NULL OR
		exportValuation IS NULL OR
		importPartnerCountry IS NULL OR
		exportPartnerCountry IS NULL OR
		importPartner2Country IS NULL OR
		exportPartner2Country IS NULL OR
		publicationNote IS NULL OR
		publicationDate IS NULL OR
		publicationDateShort IS NULL
	);
-- Individual columns is showing 0. This above might be becasue of the presence of N/A. 

-- Count for individual columns
SELECT 
	'DatasetCode' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE DatasetCode = -1
UNION ALL
SELECT 
	'typeCode' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE typeCode = 'N/A'
UNION ALL
SELECT 
	'freqCode' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE freqCode = 'N/A'
UNION ALL
SELECT 
	'period' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE period = -1
UNION ALL
SELECT 
	'reporterCode' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE reporterCode = -1
UNION ALL
SELECT 
	'reporterDescription' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE reporterDescription = 'N/A'
UNION ALL
SELECT 
	'currency' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE currency = 'N/A'
UNION ALL
SELECT 
	'importConvFactor' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE importConvFactor = -1
UNION ALL
SELECT 
	'exportConvFactor' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE exportConvFactor = -1
UNION ALL
SELECT 
	'tradeSystem' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE tradesystem = 'N/A'
UNION ALL
SELECT 
	'classificationCode' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE classificationCode = 'N/A'
UNION ALL
SELECT 
	'importValuation' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE importValuation = 'N/A'
UNION ALL
SELECT 
	'exportValuation' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE exportValuation = 'N/A'
UNION ALL
SELECT 
	'importPartnerCountry' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE importPartnerCountry = 'N/A'
UNION ALL
SELECT 
	'exportPartnerCountry' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE exportPartnerCountry = 'N/A'
UNION ALL
SELECT 
	'importPartner2Country' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE importPartner2Country = 'N/A'
UNION ALL
SELECT 
	'exportPartner2Country' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE exportPartner2Country = 'N/A'
UNION ALL
SELECT 
	'publicationNote' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE publicationNote = 'N/A'
UNION ALL
SELECT 
	'publicationDate' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE publicationDate IS NULL
UNION ALL
SELECT 
	'publicationDateShort' AS column_name,
	COUNT(*) AS null_count
FROM staging_UN_Comtrade_data
WHERE publicationDateShort IS NULL;

-- Transform data that has N/A values
-- For exportValuation, we will be replacing missing values or N/A with 'Other'.
UPDATE staging_UN_Comtrade_data
SET exportValuation = 'Other'
WHERE exportValuation = 'N/A';
-- For ImportValuation, we will be replacing missing values or N/A with 'Other'.
UPDATE staging_UN_Comtrade_data
SET exportValuation = 'Other'
WHERE exportValuation = 'N/A';
-- For tradeSystem, we will be replacing missing values or N/A with the mode (the most occuring text value).
SELECT tradeSystem, COUNT(*) AS frequency
FROM staging_UN_Comtrade_data
GROUP BY tradeSystem
ORDER BY COUNT(*) DESC
LIMIT 1; -- Output: General = 3330
-- Update the text values in 'tradeSystem'.
UPDATE staging_UN_Comtrade_data
SET tradeSystem = 'General'
WHERE tradeSystem = 'N/A';
-- Update the text values in 'importpartnerCountry'.
UPDATE staging_UN_Comtrade_data
SET importpartnerCountry = 'Unknown'
WHERE importpartnerCountry = 'N/A';
-- Update the text values in 'exportpartnerCountry'.
UPDATE staging_UN_Comtrade_data
SET exportpartnerCountry = 'Unknown'
WHERE exportpartnerCountry = 'N/A';
-- Update the text values in 'publicationNote'.
UPDATE staging_UN_Comtrade_data
SET publicationNote = 'Unknown'
WHERE publicationNote = 'N/A';
-- Update the text values in 'importPartner2Country'.
UPDATE staging_UN_Comtrade_data
SET importPartner2Country = 'Unknown'
WHERE importPartner2Country = 'N/A';
-- Update the text values in 'exportPartner2Country'.
UPDATE staging_UN_Comtrade_data
SET exportPartner2Country = 'Unknown'
WHERE exportPartner2Country = 'N/A';
-- Update the text values in 'importvaluation'.
UPDATE staging_UN_Comtrade_data
SET importvaluation = 'Unknown'
WHERE importvaluation = 'N/A';
-- There are no longer 'N/A' values in the "staging_UN_Comtrade_data" table.
-- Change the data structure for 'publicationdateshort' to fit the TIMESTAMP type.
UPDATE staging_UN_Comtrade_data
SET publicationdateshort = REPLACE(publicationdateshort, '/', '-');
-- Change the column data type  and format for 'publicationdateshort' to TIMESTAMP
ALTER TABLE staging_UN_Comtrade_data
ALTER COLUMN publicationdateshort TYPE DATE
USING to_date(publicationdateshort, 'DD-MM-YYYY');
-- Update the column to ensure the format is YYYY-MM-DD
UPDATE staging_UN_Comtrade_data
SET publicationdateshort = publicationdateshort::TEXT;
UPDATE staging_UN_Comtrade_data
SET publicationdateshort = to_date(publicationdateshort, 'YYYY-MM-DD');


-- Table: staging_GDP_data --
-- Check for null values
SELECT COUNT(*) AS null_count
FROM staging_GPD_data
WHERE 
	NOT (
		LOCATION IS NULL OR
		Country IS NULL OR
		TRANSACT IS NULL OR
		Transaction IS NULL OR
		MEASURE_Code IS NULL OR
		Measure IS NULL OR
		TIME IS NULL OR
		Year IS NULL OR
		Unit_Code IS NULL OR
		Unit IS NULL OR
		PowerCode_Code IS NULL OR
		PowerCode IS NULL OR
		Reference_Period_Code IS NULL OR
		Reference_Period IS NULL OR
		Value IS NULL OR
		Flag_Code IS NULL OR
		Flags IS NULL
	); 
-- 12088 out of 214406 have null_counts

-- Check for individual columns
SELECT 
	'Location' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data 
WHERE Location IS NULL
UNION ALL
SELECT
	'Country' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE Country IS NULL
UNION ALL
SELECT
	'TRANSACT' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE TRANSACT IS NULL
UNION ALL
SELECT
	'Transaction' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE Transaction IS NULL
UNION ALL
SELECT
	'MEASURE_Code' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE MEASURE_Code IS NULL
UNION ALL
SELECT
	'Measure' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE Measure IS NULL
UNION ALL
SELECT
	'TIME' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE TIME IS NULL
UNION ALL
SELECT
	'Year' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE Year IS NULL
UNION ALL
SELECT
	'Unit_Code' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE Unit_Code IS NULL
UNION ALL
SELECT
	'Unit' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE Unit IS NULL
UNION ALL
SELECT
	'PowerCode_Code' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE PowerCode_Code IS NULL
UNION ALL
SELECT
	'PowerCode' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE PowerCode IS NULL
UNION ALL
SELECT
	'Reference_Period_Code' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE Reference_Period_Code IS NULL
UNION ALL
SELECT
	'Reference_Period' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE Reference_Period IS NULL
UNION ALL
SELECT
	'Value' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE Value IS NULL
UNION ALL
SELECT
	'Flag_Code' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE Flag_Code IS NULL
UNION ALL
SELECT
	'Flags' AS column_name,
	COUNT(*) AS null_count
FROM staging_GPD_data
WHERE Flags IS NULL;
-- Only 4 columns have missing values (Flag_Code, Reference_Period_Code, Flags, Reference_Period).

-- Transform Data
-- We will replace the null_values with 'Unknown' in the column 'Flag_Code'.
UPDATE staging_GPD_data
SET Flag_Code = 'Unknown'
WHERE Flag_Code IS NULL;
-- We will replace the null_values with 'Unknown' in the column 'Reference_Period_Code'
UPDATE staging_GPD_data
SET Reference_Period_Code = '9999'
WHERE Reference_Period_Code IS NULL;
-- Change data type.
ALTER TABLE staging_GPD_data
ALTER COLUMN Reference_Period_Code SET DATA TYPE DATE
USING TO_DATE(Reference_Period_Code::text || '-01-01', 'YYYY-MM-DD');
-- We will replace the null_values with 'Unknown' in the column 'Flags'.
UPDATE staging_GPD_data
SET Flags = 'Unknown'
WHERE Flags IS NULL;
-- We will replace the null_values with 'Unknown' in the column 'Reference_Period'.
UPDATE staging_GPD_data
SET Reference_Period = '9999'
WHERE Reference_Period IS NULL;
-- Change data type.
ALTER TABLE staging_GPD_data
ALTER COLUMN Reference_Period SET DATA TYPE DATE
USING TO_DATE(Reference_Period::text || '-01-01', 'YYYY-MM-DD');



-- CREATE FROM THE NORMALIZATION ERD AND LOAD DATA INTO THE TABLE
-- Country Table
CREATE TABLE Country (
	country_id SERIAL PRIMARY KEY,
	country_name VARCHAR(255)
);
-- LOAD DATA
INSERT INTO country (country_name)
SELECT DISTINCT country
FROM (
	SELECT country FROM staging_historical_stock_data
	UNION
	SELECT country from staging_gpd_data
	UNION
	SELECT reporterDescription FROM staging_un_comtrade_data
) AS combined_data;
-- Check to see if it was suceessful
SELECT * FROM country;

-- Company Table
CREATE TABLE Company (
	company_id SERIAL PRIMARY KEY,
	symbol VARCHAR(255),
	name VARCHAR(255),
	ipo_year DATE,
	volume INT
);
-- LOAD DATA
INSERT INTO company (symbol, name, ipo_year, volume)
SELECT DISTINCT symbol, name, ipo_year, volume
FROM staging_historical_stock_data;
--Check if it was successful
SELECT * FROM company;


-- Sector Table
CREATE TABLE Sector (
	sector_id SERIAL PRIMARY KEY,
	sector_name VARCHAR(255)
);
-- LOAD DATA
INSERT INTO sector (sector_name)
SELECT DISTINCT sector
FROM staging_historical_stock_data;
-- Check if it was successful
SELECT * FROM sector;

-- Industry Table
CREATE TABLE Industry (
	industry_id SERIAL PRIMARY KEY,
	industry_name VARCHAR(255),
	sector_id INT REFERENCES sector(sector_id)
);
-- LOAD DATA
INSERT INTO industry (industry_name, sector_id)
SELECT DISTINCT shsd.industry, s.sector_id
FROM staging_historical_stock_data shsd
INNER JOIN sector s ON shsd.sector = s.sector_name;
-- Check if it was successful
SELECT * FROM industry;


-- Currency Table
CREATE TABLE Currency (
	currency_id SERIAL PRIMARY KEY,
	currency_code VARCHAR(255),
	curency_unit VARCHAR(255)
);
-- LOAD DATA
INSERT INTO currency(currency_code, currency_unit)
SELECT DISTINCT unit_code, unit
FROM staging_gpd_data sgd
INNER JOIN staging_UN_Comtrade_data sucd ON sucd.currency = sgd.unit_code;
-- Check if it was successful
SELECT * FROM currency;

-- Last_Sale Table
CREATE TABLE Last_Sale (
	last_sale_id SERIAL PRIMARY KEY,
	amount NUMERIC(20, 5),
	currency_id INT REFERENCES currency(currency_id),
	company_id INT REFERENCES company(company_id)
);
-- LOAD DATA
INSERT INTO last_sale (amount, currency_id, company_id)
SELECT shs.last_sale, cur.currency_id, c.company_id
FROM staging_historical_stock_data shs
JOIN currency cur ON cur.currency_code = 'USD'
JOIN company c ON shs.name = c.name;
-- Check if successful
SELECT * FROM last_sale;


-- Stock Table
CREATE TABLE Stock (
	stock_id SERIAL PRIMARY KEY,
	company_id INT REFERENCES company(company_id),
	country_id INT REFERENCES country(country_id),
	sector_id INT REFERENCES sector(sector_id),
	market_cap Numeric(20, 2),
	net_change Numeric(10, 5),
	percent_change Numeric(10, 5)
);
INSERT INTO Stock (
    company_id, country_id, sector_id, market_cap, net_change, percent_change
)
SELECT 
    c.company_id, 
    co.country_id,  
	s.sector_id,
	shs.market_cap,
    shs.net_change, 
    shs.percent_change
FROM 
    staging_historical_stock_data shs
JOIN 
    company c ON shs.name = c.name
JOIN 
    country co ON shs.country = co.country_name
JOIN 
    sector s ON shs.sector = s.sector_name;
-- Check if successful
SELECT * FROM stock;


-- Transaction Table
CREATE TABLE Transaction (
	transaction_id SERIAL PRIMARY KEY,
	transaction_code VARCHAR(255),
	transaction_type VARCHAR(255)
);
-- LOAD DATA
INSERT INTO transaction (transaction_code, transaction_type)
SELECT DISTINCT transact, transaction
FROM staging_gpd_data;
-- Check if it was successful
SELECT * FROM transaction;


-- Measure Table
CREATE TABLE Measure (
	measure_id SERIAL PRIMARY KEY,
	measure_code VARCHAR(255),
	measure_type VARCHAR(255)
);
-- LOAD DATA
INSERT INTO measure (measure_code, measure_type)
SELECT DISTINCT measure_code, measure
FROM staging_gpd_data;
-- Check if it was successful
SELECT * FROM measure;


-- GDP_Power_Code Table
CREATE TABLE GDP_Power_Code (
	power_code_id SERIAL PRIMARY KEY,
	powercode_code INT,
	powercode VARCHAR(255)
);
-- LOAD DATA
INSERT INTO GDP_Power_Code (powercode_code, powercode)
SELECT DISTINCT powercode_code, powercode
FROM staging_gpd_data;
-- Check if it was successful
SELECT * FROM GDP_Power_Code;


-- GDP_Flags Table
CREATE TABLE GDP_Flags (
	flag_id SERIAL PRIMARY KEY,
	flag_code VARCHAR(255),
	flag VARCHAR(255)
);
-- LOAD DATA
INSERT INTO GDP_Flags (flag_code, flag)
SELECT DISTINCT flag_code, flags
FROM staging_gpd_data;
-- Check if it was successful
SELECT * FROM GDP_Flags;


-- Gross_Domestic_Product Table
CREATE TABLE Gross_Domestic_Product (
	GDP_id SERIAL PRIMARY KEY,
	country_id INT REFERENCES country(country_id),
	transaction_id INT REFERENCES transaction(transaction_id),
	measure_id INT REFERENCES measure(measure_id),
	time DATE,
	power_code_id INT REFERENCES GDP_power_code(power_code_id),
	reference_period DATE,
	value NUMERIC(20),
	flag_id INT REFERENCES GDP_flags(flag_id)
);
-- LOAD DATA
INSERT INTO Gross_Domestic_Product (
    country_id, transaction_id, measure_id, time, power_code_id, reference_period, value, flag_id
)
SELECT 
	co.country_id, 
	tr.transaction_id, 
	me.measure_id, 
	sgd.time, 
	pc.power_code_id,  
	sgd.reference_period, 
	sgd.value,
	f.flag_id
FROM 
    staging_gpd_data sgd
JOIN 
    country co ON sgd.country = co.country_name
JOIN 
    transaction tr ON sgd.transact = tr.transaction_code
JOIN 
    measure me ON sgd.measure = me.measure_type
JOIN 
    gdp_power_code pc ON sgd.powercode_code = pc.powercode_code
JOIN 
    gdp_flags f ON sgd.flag_code = f.flag_code;
-- Check if successful	
SELECT * FROM Gross_Domestic_Product;


-- Trade_System Table
CREATE TABLE Trade_System (
	trade_system_id SERIAL PRIMARY KEY,
	trade_system_type Varchar(255)
);
-- LOAD DATA
INSERT INTO Trade_System (trade_system_type)
SELECT DISTINCT tradesystem
FROM staging_UN_Comtrade_data;
-- Check if it was successful
SELECT * FROM Trade_System;


-- Trade_Classification Table
CREATE TABLE Trade_Classification (
	trade_classification_id SERIAL PRIMARY KEY,
	classification_code VARCHAR(255)
);
-- LOAD DATA
INSERT INTO Trade_Classification (classification_code)
SELECT DISTINCT ClassificationCode
FROM staging_UN_Comtrade_data;
-- Check if it was successful
SELECT * FROM Trade_Classification;


-- Type_Code Table
CREATE TABLE Type_Code (
	type_code_id SERIAL PRIMARY KEY,
	type_code VARCHAR(255)
);
-- LOAD DATA
INSERT INTO Type_Code (type_code)
SELECT DISTINCT typecode
FROM staging_UN_Comtrade_data;
-- Check if it was successful
SELECT * FROM Type_Code;


-- Frequency_Code Table
CREATE TABLE Frequency_Code (
	freq_code_id SERIAL PRIMARY KEY,
	freq_code VARCHAR(255)
);
-- LOAD DATA
INSERT INTO Frequency_Code (freq_code)
SELECT DISTINCT freqcode
FROM staging_UN_Comtrade_data;
-- Check if it was successful
SELECT * FROM Frequency_Code;


-- Valuation Table
CREATE TABLE Valuation (
	valuation_id SERIAL PRIMARY KEY,
	valuation_code VARCHAR(255)
);
-- LOAD DATA
INSERT INTO Valuation (valuation_code)
SELECT DISTINCT importvaluation
FROM (
	SELECT importvaluation FROM staging_UN_Comtrade_data
	UNION
	SELECT exportvaluation from staging_UN_Comtrade_data
) AS combined_data;
-- Check if it was successful
SELECT * FROM Valuation;


-- Trade_Partner Table
CREATE TABLE Trade_Partner (
	trade_partner_id SERIAL PRIMARY KEY,
	type VARCHAR(255)
);
-- LOAD DATA
INSERT INTO Trade_Partner (type)
SELECT DISTINCT importPartnerCountry
FROM (
    SELECT importPartnerCountry AS importPartnerCountry FROM staging_UN_Comtrade_data
    UNION DISTINCT
    SELECT exportPartnerCountry AS importPartnerCountry FROM staging_UN_Comtrade_data
) AS combined_data;
-- Check if it was successful
SELECT * FROM trade_partner;



-- UN_Comtrade Table
CREATE TABLE UN_Comtrade (
	UN_comtrade_id SERIAL PRIMARY KEY,
	dataset_code NUMERIC(20),
	type_code_id INT REFERENCES type_code(type_code_id),
	freq_code_id INT REFERENCES frequency_code(freq_code_id),
	trade_period INT,
	country_id INT REFERENCES country(country_id),
	currency_id INT REFERENCES currency(currency_id),
	import_conversion_factor NUMERIC(30,20),
	export_conversion_factor NUMERIC(30, 20),
	trade_system_id INT REFERENCES trade_system(trade_system_id),
	trade_classification_id INT REFERENCES trade_classification(trade_classification_id),
	import_valuation_id INT REFERENCES valuation(valuation_id),
	export_valuation_id INT REFERENCES valuation(valuation_id),
	import_partner_country INT REFERENCES trade_partner(trade_partner_id),
	export_partner_country INT REFERENCES trade_partner(trade_partner_id),
	import_partner2_country INT REFERENCES trade_partner(trade_partner_id),
	export_partner2_country INT REFERENCES trade_partner(trade_partner_id),
	publication_note TEXT,
	publication_date TIMESTAMP
);
-- LOAD DATA
INSERT INTO UN_Comtrade (
    dataset_code, type_code_id, freq_code_id, trade_period, country_id, currency_id, import_conversion_factor, export_conversion_factor,
	trade_system_id, trade_classification_id, import_valuation_id, export_valuation_id, import_partner_country, 
	export_partner_country, import_partner2_country, export_partner2_country, publication_note, publication_date 
)
SELECT 
	scd.datasetcode, 
	tc.type_code_id, 
	fc.freq_code_id, 
	scd.period, 
	cou.country_id,
	cur.currency_id, 
	scd.importConvFactor, 
	scd.exportConvFactor,
	ts.trade_system_id, 
	tcc.trade_classification_id,
	imp_val.valuation_id AS import_valuation_id, 
    exp_val.valuation_id AS export_valuation_id,
	imp_pt.trade_partner_id AS import_partner_country, 
    exp_pt.trade_partner_id AS export_partner_country, 
    imp_pt2.trade_partner_id AS import_partner2_country, 
    exp_pt2.trade_partner_id AS export_partner2_country,
	scd.publicationnote, 
	scd.publicationdate 
FROM 
    staging_UN_Comtrade_data scd
LEFT JOIN 
    type_code tc ON scd.typecode = tc.type_code
LEFT JOIN 
    frequency_code fc ON scd.freqcode = fc.freq_code
LEFT JOIN 
    country cou ON scd.reporterdescription = cou.country_name
LEFT JOIN 
    currency cur ON scd.currency = cur.currency_code
LEFT JOIN 
    trade_system ts ON scd.tradesystem = ts.trade_system_type
LEFT JOIN 
    trade_classification tcc ON scd.classificationcode = tcc.classification_code
LEFT JOIN
    valuation imp_val ON scd.exportvaluation = imp_val.valuation_code
LEFT JOIN
    valuation exp_val ON scd.exportvaluation = exp_val.valuation_code
LEFT JOIN 
    trade_partner imp_pt ON scd.importpartnercountry = imp_pt.type
LEFT JOIN 
    trade_partner exp_pt ON scd.exportpartnercountry = exp_pt.type
LEFT JOIN 
    trade_partner imp_pt2 ON scd.importpartner2country = imp_pt2.type
LEFT JOIN 
    trade_partner exp_pt2 ON scd.exportpartner2country = exp_pt2.type;	
-- check if it was successful
SELECT * FROM UN_comtrade;
		


-- INDEX PRIMARY KEYS AND COLUMNS USED IN THE DIMENSIONAL MODEL TO OPTIMIZE PERFORMANCE
CREATE INDEX idx_country ON country(country_id, country_name);

CREATE INDEX idx_company ON company(company_id, symbol, name, IPO_year, volume);

CREATE INDEX idx_sector ON sector(sector_id, sector_name);

CREATE INDEX idx_gross_domestic_product ON gross_domestic_product(GDP_id, value);



-- DESIGN FROM THE DIMENSIONAL MODEL
-- Dimensional Tables
-- Time_Dimension
CREATE TABLE Time_Dimension (
    Time_ID SERIAL PRIMARY KEY,
    Time_Date DATE,
    Time_Year INT,
    Time_Month INT,
    Time_Day INT
);
-- DROP PROCEDURE Load_Time_Dimension_scd1(SP_OP TEXT);
--LOAD DATA USING STORED PROCEDURE
CREATE PROCEDURE Load_Time_Dimension_scd1(SP_OP TEXT)
LANGUAGE plpgSQL
AS $$
BEGIN
    IF SP_OP = 'INSERT' THEN
        INSERT INTO Time_Dimension (Time_Date, Time_Year, Time_Month, Time_Day)
        SELECT DISTINCT 
            TO_DATE(CAST(EXTRACT(YEAR FROM IPO_Year) AS TEXT) || '-' || 
                    CAST(EXTRACT(MONTH FROM IPO_Year) AS TEXT) || '-' || 
                    CAST(EXTRACT(DAY FROM IPO_Year) AS TEXT), 'YYYY-MM-DD') AS Time_Year,
            EXTRACT(YEAR FROM IPO_Year) AS Time_Year,
            EXTRACT(MONTH FROM IPO_Year) AS Time_Month,
            EXTRACT(DAY FROM IPO_Year) AS Time_Day
        FROM company;
    ELSIF SP_OP = 'UPDATE' THEN
        UPDATE Time_Dimension AS TD
        SET 
            Time_Date = TO_DATE(CAST(EXTRACT(YEAR FROM Coy.IPO_Year) AS TEXT) || '-' || 
                                CAST(EXTRACT(MONTH FROM Coy.IPO_Year) AS TEXT) || '-' || 
                                CAST(EXTRACT(DAY FROM Coy.IPO_Year) AS TEXT), 'YYYY-MM-DD'),
            Time_Year = EXTRACT(YEAR FROM Coy.IPO_Year),
            Time_Month = EXTRACT(MONTH FROM Coy.IPO_Year),
            Time_Day = EXTRACT(DAY FROM Coy.IPO_Year)
        FROM company AS Coy
        WHERE 
            TD.Time_ID = Coy.IPO_Year AND
            (TD.Time_Year <> EXTRACT(YEAR FROM Coy.IPO_Year) OR
             TD.Time_Month <> EXTRACT(MONTH FROM Coy.IPO_Year) OR
             TD.Time_Day <> EXTRACT(DAY FROM Coy.IPO_Year));
    END IF;
END;
$$;
-- execute the stored procedure
CALL Load_Time_Dimension_scd1('INSERT');
CALL Load_Time_Dimension_scd1('UPDATE');
-- Check loaded table
SELECT * FROM Time_Dimension;


--## Country_Dimension and Load from 'Country' Table ##--
CREATE TABLE Country_Dimension (
	Country_id INT PRIMARY KEY,
	Country_name VARCHAR(255)
);

-- DROP PROCEDURE Load_Country_Dimension_scd1();
--LOAD DATA USING STORED PROCEDURE
CREATE PROCEDURE Load_Country_Dimension_scd1()
LANGUAGE plpgSQL
AS $$
BEGIN
	-- Load data into the table from the normalized table
	INSERT INTO Country_Dimension (Country_id, Country_name)
	SELECT Country_id, Country_name
	FROM Country;
	
	-- Update existing sectors in Sector_Dimension
    UPDATE Country_Dimension AS CD
    SET Country_name = C.Country_name
    FROM Country AS C
    WHERE CD.country_id = C.country_id
    AND CD.country_name <> C.country_name;
END;
$$;
-- execute the stored procedure
CALL Load_Country_Dimension_scd1();
-- Check if it was successful.
SELECT * FROM Country_Dimension;


--## Sector_Dimension and Load from 'Sector' Table ##--
CREATE TABLE Sector_Dimension (
	Sector_id INT PRIMARY KEY,
	Sector_name VARCHAR(255)
);
-- DROP PROCEDURE Load_Overwrite_Sector_Dimension_scd1();
--LOAD DATA USING STORED PROCEDURE
CREATE PROCEDURE Load_Overwrite_Sector_Dimension_scd1()
LANGUAGE plpgSQL
AS $$
BEGIN
    -- Insert new sectors into Sector_Dimension
    INSERT INTO Sector_Dimension(Sector_id, Sector_name)
    SELECT sector_id, sector_name FROM Sector
    ON CONFLICT (Sector_id) DO NOTHING; -- Ignore if record already exists

    -- Update existing sectors in Sector_Dimension
    UPDATE Sector_Dimension AS SD
    SET Sector_name = S.sector_name
    FROM Sector AS S
    WHERE SD.Sector_id = S.sector_id
    AND SD.Sector_name <> S.sector_name;
END;
$$;
-- execute the stored procedure
CALL Load_Overwrite_Sector_Dimension_scd1();
-- Check loaded table
SELECT * FROM Sector_Dimension;


--## Stock_Company_Dimension ##--
CREATE TABLE Stock_Company_Dimension (
	Stock_Company_ID INT PRIMARY KEY,
	Stock_Symbol VARCHAR(255),
	Stock_Name VARCHAR(255),
	Stock_IPO_Year DATE,
	Stock_Volume INT,
	current_flag BOOLEAN,
	Effective_Timestamp TIMESTAMP,
	Expiration_Timestamp TIMESTAMP
);

--DROP PROCEDURE Load_Update_Stock_Company_Dimension_scd2(SP_OP TEXT);
--LOAD DATA USING STORED PROCEDURE
CREATE OR REPLACE PROCEDURE Load_Update_Stock_Company_Dimension_scd2(SP_OP TEXT)
LANGUAGE plpgsql
AS $$
BEGIN 
    IF SP_OP = 'INSERT' THEN
        INSERT INTO Stock_Company_Dimension(
            Stock_Company_ID,
            Stock_Symbol,
            Stock_Name,
            Stock_IPO_Year,
			Stock_Volume,
			Current_Flag, 
            Effective_Timestamp,
            Expiration_Timestamp
        )
        SELECT
            c.company_id AS Stock_Company_ID,
            c.symbol AS Stock_Symbol,
            c.name AS Stock_Name,
            c.IPO_year AS Stock_IPO_Year,
			c.volume AS Stock_Volume,
			true AS current_flag,
            CURRENT_TIMESTAMP AS Effective_Timestamp,
            'infinity'::timestamp AS Expiration_Timestamp
        FROM company c;
				
    ELSIF SP_OP = 'UPDATE' THEN
        UPDATE Stock_Company_Dimension
        SET 
            Expiration_Timestamp = CURRENT_TIMESTAMP,
			current_flag = 
				CASE
					WHEN Expiration_Timestamp = 'infinity' THEN true
					ELSE false
				END
        WHERE 
            Expiration_Timestamp = 'infinity'::timestamp;

        INSERT INTO Stock_Company_Dimension(
            Stock_Company_ID,
            Stock_Symbol,
            Stock_Name,
            Stock_IPO_Year,
			Stock_Volume,
			Current_Flag,
            Effective_Timestamp,
            Expiration_Timestamp
        )
        SELECT
            c.company_id AS Stock_Company_ID,
            c.symbol AS Stock_Symbol,
            c.name AS Stock_Name,
            c.IPO_year AS Stock_IPO_Year,
			c.volume AS Stock_Volume,
			true AS current_flag,
            CURRENT_TIMESTAMP AS Effective_Timestamp,
            'infinity'::timestamp AS Expiration_Timestamp
        FROM company c;
    END IF;
END;
$$;
-- Execute the procedure for insert operation
CALL Load_Update_Stock_Company_Dimension_scd2('INSERT');
-- Execute the procedure for update operation
CALL Load_Update_Stock_Company_Dimension_scd2('UPDATE');
-- Check if it was successful.
SELECT * FROM Stock_Company_Dimension;


--## GDP_Dimension ##--
CREATE TABLE GDP_Dimension (
	GDP_ID SERIAL PRIMARY KEY,
	GDP_Value NUMERIC(20)
);
-- DROP PROCEDURE Load_GDP_Dimension_scd1(SP_OP TEXT);
-- LOAD THE TABLE USING STORED PROCEDURES
CREATE OR REPLACE PROCEDURE Load_GDP_Dimension_scd1(SP_OP TEXT)
LANGUAGE plpgSQL
AS $$
BEGIN
	IF SP_OP = 'INSERT' THEN
		INSERT INTO GDP_Dimension(GDP_value)
		SELECT
			g.value 
		FROM Gross_Domestic_Product AS g;
	
	ELSIF SP_OP = 'UPDATE' THEN
		UPDATE GDP_Dimension AS GD
    	SET GDP_Value = g.value
   		FROM Gross_Domestic_Product AS g
   		WHERE GD.gdp_id = g.gdp_id AND GD.gdp_value <> g.value;
	END IF;
END;
$$;
-- Execute the procedure 
CALL Load_GDP_Dimension_scd1('INSERT');
-- Check if it was successful.
SELECT * FROM GDP_Dimension;




------####### FACT TABLE #######------
CREATE TABLE Trade_Unified_Fact(
	Time_ID BIGINT REFERENCES Time_Dimension(Time_ID),
	Country_ID BIGINT REFERENCES Country_Dimension(Country_ID),
	Sector_ID BIGINT REFERENCES Sector_Dimension(Sector_ID),
	Stock_Company_ID BIGINT REFERENCES Stock_Company_Dimension(Stock_Company_ID),
	GDP_ID BIGINT REFERENCES GDP_Dimension(GDP_ID),
	Trade_Volume NUMERIC, 
	IPO_Count NUMERIC,
	GDP_Trend NUMERIC,
	GDP_Growth_Rate NUMERIC
	-- PRIMARY KEY (Time_ID, COuntry_ID, Sector_ID, Stock_Company_ID, GDP_ID)
	-- CONSTRAINT trade_unified_fact_unique_constraint UNIQUE (time_id, country_id, sector_id, stock_company_id, GDP_id)
);
SELECT * FROM trade_unified_fact;
-- DROP TABLE trade_unified_fact;



------####### MEASURE 1 #######------
--DROP PROCEDURE Calculate_Load_Trade_Volume();
-- CALCULATE AND LOAD TRADE_VOLUME
CREATE PROCEDURE Calculate_Load_Trade_Volume()
LANGUAGE plpgSQL
AS $$
BEGIN
	-- Delete all existing records from the fact table
	DELETE FROM Trade_Unified_Fact;
	--Insert Trade Volume into the fact table
	INSERT INTO Trade_Unified_Fact (Time_id, sector_id, trade_volume)
	SELECT
		td.time_id,
		sd.sector_id,
		SUM(scd.stock_volume) AS trade_volume
	FROM 
		stock ss
	LEFT JOIN 
		stock_company_dimension scd ON ss.company_id = scd.stock_company_id
	LEFT JOIN 
		time_dimension td ON scd.stock_ipo_year = td.time_date
	LEFT JOIN 
		sector_dimension sd ON ss.sector_id = sd.sector_id
	WHERE sd.sector_name = 'Health Care'
	GROUP BY
		sd.sector_id,
		td.time_id
	ORDER BY
		sd.sector_id,
		td.time_id,
		trade_volume;
END;
$$;
-- Execute the procedure
CALL Calculate_Load_Trade_Volume();
-- Check if successful
SELECT * FROM Trade_Unified_Fact;
--Simple query to test
SELECT DISTINCT
	td.time_date, 
	sd.sector_name, 
	trade_volume
FROM 
	trade_unified_fact AS tuc
LEFT JOIN 
	time_dimension td ON tuc.time_id = td.time_id
LEFT JOIN 
	sector_dimension sd ON tuc.sector_id = sd.sector_id
WHERE sd.sector_name = 'Health Care';



------####### MEASURE 2 #######------
--DROP PROCEDURE Calculate_Load_IPO_Count();
-- CALCULATE AND LOAD IPO_COUNT
CREATE PROCEDURE Calculate_Load_IPO_Count()
LANGUAGE plpgSQL
AS $$
BEGIN
	-- Delete all existing records from the fact table
	DELETE FROM Trade_Unified_Fact;
	--Insert IPO_Count into the fact table
    INSERT INTO Trade_Unified_Fact (Time_id, country_id, ipo_count)
    SELECT
		td.time_id,
		cs.country_id,
        COUNT(scd.stock_ipo_year) AS ipo_count 
    FROM
        Stock st
	LEFT JOIN
        Country_Dimension cs ON st.country_id = cs.country_id
	LEFT JOIN
		Stock_Company_Dimension scd ON st.company_id = scd.stock_company_id
	LEFT JOIN
        Time_Dimension td ON scd.stock_ipo_year = td.Time_date
	-- WHERE td.time_year = '2023'
	GROUP BY
        td.Time_id,
        cs.country_id
	ORDER BY
		ipo_count DESC;
	-- ON CONFLICT (time_id, country_id, ipo_count) DO NOTHING; -- Ignore if sector already exists
END;
$$;
-- Execute the procedure
CALL Calculate_Load_IPO_Count();
-- Check if successful
SELECT * FROM Trade_Unified_Fact;
-- Check what country the country_id '224' belongs to.
--Simple query to test
SELECT DISTINCT
	td.time_date, 
	cd.country_name, 
	IPO_Count
FROM 
	trade_unified_fact AS tuc
LEFT JOIN 
	time_dimension td ON tuc.time_id = td.time_id
LEFT JOIN 
	country_dimension cd ON tuc.country_id = cd.country_id
WHERE td.time_year = '2023';




------####### MEASURE 3 #######------
--DROP PROCEDURE Calculate_Load_GDP_Trend();
-- CALCULATE AND LOAD GDP_Trend
CREATE PROCEDURE Calculate_Load_GDP_Trend()
LANGUAGE plpgsql
AS $$
BEGIN
	-- Delete all existing records from the fact table
	DELETE FROM Trade_Unified_Fact;
    -- Insert GDP trend data
    INSERT INTO Trade_Unified_Fact (Time_id, country_id, gdp_trend)
    SELECT
        td.Time_id,
        g.country_id,
        AVG(g.value) AS gdp_trend
    FROM
		GDP_dimension gd
	LEFT JOIN
        gross_domestic_product g ON gd.gdp_value = g.value
    LEFT JOIN
        Time_Dimension td ON g.time = td.time_date
    LEFT JOIN
        Country_Dimension cy ON g.country_id = cy.country_id
	-- ON CONFLICT (time_id, country_id, gdp_trend) DO NOTHING; -- Ignore if sector already exists
	-- WHERE country_name = 'United States'
	GROUP BY
        td.Time_id,
        g.country_id
	ORDER BY
		td.time_id,
		g.country_id, 
		gdp_trend;
END;
$$;
-- Execute the procedure
CALL Calculate_Load_GDP_Trend();
-- Check if successful
SELECT * FROM Trade_Unified_Fact;
-- Test and Validation
SELECT DISTINCT
	td.time_date, 
	cd.country_name, 
	gdp_trend
FROM 
	trade_unified_fact AS tuc
LEFT JOIN 
	time_dimension td ON tuc.time_id = td.time_id
LEFT JOIN 
	country_dimension cd ON tuc.country_id = cd.country_id
WHERE td.time_year = '2021';
	
	

------####### MEASURE 4 #######------
--DROP PROCEDURE Calculate_Load_GDP_Growth_Rate();
-- CALCULATE AND LOAD GDP_Growth_Rate
CREATE PROCEDURE Calculate_Load_GDP_Growth_Rate()
LANGUAGE plpgsql
AS $$
BEGIN
	-- Delete all existing records from the fact table
	DELETE FROM Trade_Unified_Fact;
    -- Insert GDP growth rate data
        INSERT INTO Trade_Unified_Fact (Time_id, country_id, gdp_growth_rate)
    SELECT
        td.Time_id,
        g.country_id,
		-- Calculate GDP growth rate with the formula below:
		-- GDP Growth Rate= (Current Year’s GDP − Previous Year’s GDP) / Previous Year’s GDP
        CASE
            WHEN lag(g.value) OVER (PARTITION BY g.country_id ORDER BY td.time_id) = 0 THEN NULL -- Handle division by zero
            ELSE (g.value - lag(g.value) OVER (PARTITION BY g.country_id ORDER BY td.time_id)) / lag(g.value) OVER (PARTITION BY g.country_id ORDER BY td.time_id)
        END AS gdp_growth_rate
		-- LAG function retrieves the value from the previous row within the same partition (partitioned by country) ordered by time
    FROM
        gross_domestic_product g
    JOIN
        Time_Dimension td ON g.time = td.time_date
    JOIN
        Country_Dimension cy ON g.country_id = cy.country_id
    --WHERE time_year = 2023
	GROUP BY
        td.Time_id,
        g.country_id,
        g.value;

END;
$$;
-- Execute the procedure
CALL Calculate_Load_GDP_Growth_Rate();
-- Check if successful
SELECT * FROM Trade_Unified_Fact;
-- Test and Validation
SELECT DISTINCT
	td.time_date, 
	cd.country_name, 
	GDP_Growth_Rate
FROM 
	trade_unified_fact AS tuc
LEFT JOIN 
	time_dimension td ON tuc.time_id = td.time_id
LEFT JOIN 
	country_dimension cd ON tuc.country_id = cd.country_id
WHERE td.time_year = '2021';

-- Test and Validation 2
SELECT DISTINCT time_id, country_id, MAX(GDP_Growth_Rate)
FROM Trade_Unified_Fact
GROUP BY
	time_id,
	country_id;



