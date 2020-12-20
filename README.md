# UN_Population_Data
UN Population data analysis using apache pig
You are given the “unsd-citypopulation-year-fm.csv” dataset. Perform the following analysis using Pig:

Question 1: Find the number of countries in the dataset.

Question 2: List the countries together with the number of cities in each country<sup>1</sup>

Question 3: List countries in ascending order of female-to-male ratio, throughout
the years<sup>2</sup>

Question 4: List the top 10 most populated cities according to the most recent data
in the dataset<sup>3</sup>

Question 5: List the top 10 cities which have the highest population change per
year in percentage since the start of the survey<sup>4</sup>



Notes:

1. We only count cities which are in the dataset.

2. You should have one entry for each country (not city) in each year.

3. Use the most recent figures of a city. If possible, do not fix the year but let your program find out what the most
recent population is for each city. As we cannot guarantee that data are available for all cities in each year, you may
end up comparing city A’s population with city B’s in different years. This is fine as far as the figures are the most
recent ones for both A and B.

4. By “population change per year in percentage” since “the start of the survey”, we mean: If the earliest figure of city
X is P0 and the latest figure is Pn after N years, the total change in percentage C=(Pn-P0)/P0*100% over N years. And
the change per year is C/N. We need to take the number of years into consideration as we cannot guarantee that all
cities’ data span the same period of time. e.g. It makes no sense to compare city A’s change in 1 year with city B’s
change in 10 year.
