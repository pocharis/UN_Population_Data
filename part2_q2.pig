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

country_city = foreach un_dataset GENERATE Country, City;

--checking for missing or null values in the data
country_city = FILTER country_city  BY  Country != ' ' AND 
				     Country != '' AND 
				     City IS NOT NULL AND
				     Country IS NOT NULL;

--obtaining a unique record for each country and city
unique_country_city = DISTINCT country_city;


--grouping by country, to get their respecive city records
group_country_year = GROUP unique_country_city BY Country;



--looping to count the cities for each country
country_year_cityCount = FOREACH group_country_year{
				GENERATE group as Country, 
				COUNT(unique_country_city.City) AS City_cnt;
				};


-- *OPTIONAL* ordering the list of countries and city, to see the country with most city count
orderedCountryCityList = ORDER country_year_cityCount BY City_cnt DESC;


--displaying the result
DUMP orderedCountryCityList;
 

--uncomment the line below to store the result of this query
--STORE orderedCountryCityList into 'orderedCountryCityList';
