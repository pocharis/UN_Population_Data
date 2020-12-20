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
data_analyse = FOREACH un_dataset GENERATE Country, Year, Sex, Value;

--checking for missing or null values in the data
data_analyse_clean = FILTER data_analyse  BY  Country != ' ' AND 
				     Country != '' AND 
				     Value IS NOT NULL AND
				     Country IS NOT NULL;

--splitting into two data sets based on sex
SPLIT data_analyse_clean INTO male_population IF Sex == 'Male', female_population IF Sex == 'Female';


--grouping by country, year in order to add the values 
male_pop_group = GROUP male_population BY (Country, Year);

--adding up the values for cities with two city type records
sum_male_popul = FOREACH male_pop_group {
		GENERATE FLATTEN(group) AS (Country, Year), SUM(male_population.Value) AS Value;
};





--grouping by country, year  in order to add the values 
female_pop_group = GROUP female_population BY (Country, Year);

--adding up the values for cities with two city type records
sum_female_popul = FOREACH female_pop_group {
		GENERATE FLATTEN(group) AS (Country, Year), SUM(female_population.Value) AS Value;
};


--joining the two datasets on common attributes
combined = JOIN sum_male_popul by (Country, Year), sum_female_popul by (Country, Year);


--obtaining the female to male ratio by, type casting to float and then  2 decimal places for the ratio.
femaleMaleRatio = FOREACH combined {
        			ratio = (float)sum_female_popul::Value /
		                         (float)sum_male_popul::Value;
        			GENERATE sum_female_popul::Country AS Country,
        				 sum_female_popul::Year AS Year, 
					 ROUND(ratio * 100.00)/100.00 AS Ratio;
}

--ascending order of female to male ratio
orderedFemaleMaleRatio = ORDER femaleMaleRatio BY Ratio ASC;



--displaying the result
DUMP orderedFemaleMaleRatio;

--uncomment the line below to store the result of this query
--STORE orderedFemaleMaleRatio into 'orderedFemaleMaleRatio';
