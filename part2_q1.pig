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



list_of_country = foreach un_dataset GENERATE Country;

--checking for missing or null values in the data
list_of_country = FILTER list_of_country  BY  Country != ' ' AND 
				     Country != '' AND 
				     Country IS NOT NULL;

--obtaining a unique record for each country
unique_list_country = DISTINCT list_of_country;


--to count, grouping is used for all the countries
unique_group = GROUP unique_list_country ALL;


--getting a count of each country, using the COUNT() method 
count_country = FOREACH unique_group GENERATE COUNT(unique_list_country);


--displaying the result
DUMP count_country;


