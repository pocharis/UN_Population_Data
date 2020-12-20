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


--grouping to obtain the most recent year
city_country = GROUP city_year_total BY (Country, City);



--taking  the most recent year after ordering in descending order
most_recent = FOREACH city_country{
			sorted = ORDER city_year_total BY Year DESC;
			Top = LIMIT sorted 1;
			GENERATE FLATTEN(Top);
	};

--ordering by the most recent year
orderedPopulation = ORDER most_recent BY Top::Value DESC;

--taking the top 10
top10Cities = LIMIT orderedPopulation 10;

--displaying the result
DUMP top10Cities;

