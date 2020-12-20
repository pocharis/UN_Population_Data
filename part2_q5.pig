--Registering and defining the UDF for the CSV
REGISTER /usr/lib/pig/piggybank.jar

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

--loading the dataset and defining the attributes and their respective types
un_dataset = LOAD 'unsd-citypopulation-year-fm.csv' USING CSVLoader() AS (
				Country:chararray,
                		Year:int,
				Area:chararray, 
				Sex:chararray,
				City:chararray,
				City_type:chararray,
				Record_type:chararray, 
				Reliability:chararray, 
				Source_year:int,
                		Value:int, 
				Value_footnotes:int);

--generating the required attributes for analysis
data_analyse = FOREACH un_dataset GENERATE Country, Year,City, Value;

--checking for missing or null values in the data
data_analyse_clean = FILTER data_analyse  BY  Country != ' ' AND 
				     Country != '' AND 
				     Value IS NOT NULL AND
				     Country IS NOT NULL;



--grouping, so to cum the population for the various cities and countries in a year
grouped_cities_country = GROUP data_analyse_clean BY (Country,Year, City);     

--summing up the population values
city_year_total = FOREACH grouped_cities_country{
				Value = SUM(data_analyse_clean.Value);
				generate FLATTEN(group) AS (Country,Year, City), Value AS Value;
	};

cityGroup = GROUP city_year_total BY City;


--extracting the high and low years and population values by ordering in descending order and taking the first value
topCitiesObtained = FOREACH cityGroup{
		sortDesc = ORDER city_year_total BY Year DESC;
		limitDesc = LIMIT sortDesc 1;

		sortAsc = ORDER city_year_total BY Year ASC;
		limitAsc = LIMIT sortAsc 1;


		GENERATE group AS City, FLATTEN(limitAsc) AS 
						(Country,  bottomYear, city ,bottomValue), 
					FLATTEN(limitDesc) AS 
						(country, topYear,citY, topValue);
 
			 
};


--taking only the countries with more than one year change
topCitiesFilter = FILTER topCitiesObtained BY (topYear - bottomYear > 1);


--computing the population change and year difference 
computation = FOREACH topCitiesFilter{

	Pn_P0 = (topValue - bottomValue);
	C = (float)Pn_P0 / (float)bottomValue;
	cPercent = C * 100;

	N = topYear - bottomYear;
	C_N = cPercent / N ;
	
	GENERATE  Country AS Country, City AS City,  ROUND(C_N * 100.00)/100.00 AS C_N ;
};

--ordering in descending order, to obtain the top 10
orderedChangePerYear = ORDER computation BY C_N DESC;

--taking the top 10 cities by population change
top10CitiesChange = LIMIT orderedChangePerYear 10;

--Displaying Result
DUMP top10CitiesChange;

