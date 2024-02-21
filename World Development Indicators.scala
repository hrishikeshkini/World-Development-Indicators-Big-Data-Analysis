// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC # Let's discover more about the World Development Indicators!
// MAGIC
// MAGIC * The primary World Bank collection of development indicators, compiled from officially-recognized international sources. 
// MAGIC * It presents the most current and accurate global development data available, and includes national, regional and global estimates.
// MAGIC

// COMMAND ----------

// MAGIC %md ## World Development Indicators
// MAGIC
// MAGIC #### Explore country development indicators from around the world 

// COMMAND ----------

// DBTITLE 1,Country Data - Data frame Definition

val country = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/Country.csv")

display(country)

// COMMAND ----------

// DBTITLE 1,Create Temp View of Country Data

country.createOrReplaceTempView("country")

// COMMAND ----------


country.printSchema()

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select * from country;

// COMMAND ----------

// DBTITLE 1,Indicators Data - Data frame Definition

val Indicators = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/Indicators.csv")

display(Indicators)

// COMMAND ----------

// DBTITLE 1,Create Temp View on Indicators Data
Indicators.createOrReplaceTempView("Indicators")

// COMMAND ----------

// DBTITLE 1,What is the Gini Index?
// MAGIC %md
// MAGIC
// MAGIC ## What is Gini Index?
// MAGIC
// MAGIC The Gini index is a simple measure of the distribution of income across income percentiles in a population.
// MAGIC A higher Gini index indicates greater inequality, with high income individuals receiving much larger percentages of the total income of the population.

// COMMAND ----------

// DBTITLE 1,GINI Index of China
// MAGIC %sql
// MAGIC
// MAGIC   select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "CHN" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Argentina
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "ARG" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Uganda
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "UGA" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of USA
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "USA" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Colombia
// MAGIC %sql 
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "COL" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Rwanda
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "RWA" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Russia
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "RUS" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Ecuador
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "ECU" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Central African Republic
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "CAF" order by Year asc;

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## What is Youth Literacy Rate
// MAGIC Youth literacy rate refers to the percentage of people within a specific age group, typically between the ages of 15 and 24, who can read and write. This indicator is often used to assess the level of education and literacy among young adults in a given population. A higher youth literacy rate generally indicates better educational opportunities and resources for young people, while a lower rate may suggest challenges in providing access to education.
// MAGIC

// COMMAND ----------

// DBTITLE 1,Youth Literacy Rate in 1990
// MAGIC %sql 
// MAGIC
// MAGIC select value as Youth_Literacy_Rate,ShortName from Indicators A JOIN Country N ON A.CountryCode = N.CountryCode  where IndicatorCode  = "SE.ADT.1524.LT.ZS" and Year = 1990 order by Youth_Literacy_Rate Desc; 

// COMMAND ----------

// DBTITLE 1,Youth Literacy Rate in 2010
// MAGIC %sql 
// MAGIC
// MAGIC select value as Youth_Literacy_Rate,ShortName from Indicators A JOIN Country N ON A.CountryCode = N.CountryCode  where IndicatorCode  = "SE.ADT.1524.LT.ZS" and Year = 2010 order by Youth_Literacy_Rate Desc; 

// COMMAND ----------

// MAGIC %md
// MAGIC ## What is trade to GDP ratio
// MAGIC The ratio of trade to Gross Domestic Product (GDP) is commonly known as the trade-to-GDP ratio. It is expressed as a percentage and represents the total value of a country's exports and imports in relation to its GDP.

// COMMAND ----------

// DBTITLE 1,Trade as a percentage of GDP for China and India
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "NE.TRD.GNFS.ZS" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// DBTITLE 1,Exports of goods and services (constant 2005 US$)
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "NE.EXP.GNFS.KD" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// DBTITLE 1,Import of goods and services (constant 2005 US$)
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "NE.IMP.GNFS.KD" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// MAGIC %md
// MAGIC ## What is GDP per capita adjusted by Purchasing Power Parity (PPP)
// MAGIC Gross Domestic Product (GDP) per capita adjusted by Purchasing Power Parity (PPP) is a measure of the average economic output per person in a country, taking into account differences in the cost of living and inflation rates. It provides a more accurate comparison of living standards and economic well-being between different countries than nominal GDP per capita.

// COMMAND ----------

// DBTITLE 1,GDP per capita (adjusted by purchasing power parity)
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "NY.GDP.PCAP.PP.KD" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// MAGIC %md
// MAGIC ## What is Poverty Alleviation
// MAGIC Poverty alleviation refers to the set of measures and strategies aimed at reducing, alleviating, or eradicating poverty within a given population or community. The goal is to improve the economic and social well-being of individuals and households living in poverty by providing them with the means to escape poverty's cycle.

// COMMAND ----------

// DBTITLE 1,Poverty Alleviation
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "SI.POV.2DAY" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// MAGIC %md
// MAGIC ## What is Life Expectancy at birth
// MAGIC Life expectancy at birth is a statistical measure that represents the average number of years a person can expect to live if subjected to current mortality rates throughout their entire life. It is typically expressed in years and serves as an indicator of the overall health and mortality conditions within a specific population. Life expectancy at birth is influenced by factors such as healthcare, nutrition, sanitation, living standards, and access to education.

// COMMAND ----------

// DBTITLE 1,Life Expectancy at birth, total (years)
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "SP.DYN.LE00.IN" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// MAGIC %md
// MAGIC ## What is Urban Population growth
// MAGIC Urban population growth refers to the rate at which the population living in urban areas increases over time. It is a demographic indicator that reflects the migration of people from rural areas to cities and towns, as well as natural population growth within urban centers. Urbanization is the process by which an increasing proportion of a country's population resides in urban areas, and urban population growth is a key aspect of this phenomenon.

// COMMAND ----------

// DBTITLE 1,Urban Population growth
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "SP.URB.TOTL.IN.ZS" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// MAGIC %md
// MAGIC ## What is Infant Mortality - as a measure of health care
// MAGIC Infant mortality is a critical indicator of healthcare quality and overall health system performance. It refers to the number of deaths of infants (children under one year of age) per 1,000 live births in a given population during a specific time period. The infant mortality rate is widely used to assess the effectiveness of healthcare services, public health interventions, and socio-economic conditions that influence child health.

// COMMAND ----------

// DBTITLE 1,Infant Mortality - as a measure of health care
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  
// MAGIC from Indicators 
// MAGIC where IndicatorCode = "SH.DTH.IMRT" 
// MAGIC   and CountryCode in ("IND", "CHN") 
// MAGIC   and Year > 1968 
// MAGIC order by Year;

// COMMAND ----------

// MAGIC %md
// MAGIC ## What is Average Income
// MAGIC Average income refers to the total income earned by a population divided by the number of people in that population, providing a measure of the average economic well-being. Income levels can vary widely between countries and regions due to differences in economic development, cost of living, and other factors.

// COMMAND ----------

// DBTITLE 1,The 10 Countries with Lowest Average Income in 1962
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and Year = 1962 order by Value asc limit 10;

// COMMAND ----------

// DBTITLE 1,The 10 Countries with Lowest Average Income in 2014
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and Year = 2014 order by Value asc limit 10;

// COMMAND ----------

// DBTITLE 1,The 10 Countries with Highest Average Income in 1962
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and Year = 1962 order by Value desc limit 10;

// COMMAND ----------

// DBTITLE 1,The 10 Countries with Highest Average Income in 2014
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and Year = 2014 order by Value desc limit 10;

// COMMAND ----------

// DBTITLE 1,Average Income from 1960-2014 in Rich Countries
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value,Year from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and CountryName in("Australia","Austria","Canada", "Luxembourg", "Netherlands","Norway","United States") 
// MAGIC

// COMMAND ----------

// DBTITLE 1,Average Income from 1960-2014 in Poor Countries
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value,Year from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and CountryName in("Burundi","Togo","Malawi","Central African Republic") 
// MAGIC

// COMMAND ----------

// DBTITLE 1,Average Income in 1962
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value,Year from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and Year = 1962 and CountryName in ("Malawi","China","Luxembourg","United States") order by Value asc;

// COMMAND ----------

// DBTITLE 1,Average Income in 2014
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value,Year from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and Year = 2014 and CountryName in ("Malawi","China","Luxembourg","United States") order by Value asc;

// COMMAND ----------

// MAGIC %md
// MAGIC ## What is Life Expectancy
// MAGIC  Life expectancy is the average number of years a person can expect to live, and it is influenced by various factors such as healthcare, nutrition, sanitation, living standards, and more.

// COMMAND ----------

// DBTITLE 1,Life Expectancy in France 1960-2013
// MAGIC %sql 
// MAGIC SELECT year,Value
// MAGIC     FROM Indicators
// MAGIC     WHERE CountryName = "India"
// MAGIC     AND IndicatorCode = "SP.DYN.LE00.IN";

// COMMAND ----------

// MAGIC %md
// MAGIC ## What is Birth Rates
// MAGIC Birth rates refer to the number of live births per 1,000 people in a given population over a specific period, usually a year. Birth rates can vary widely between countries and are influenced by factors such as socio-economic conditions, cultural norms, access to healthcare, education, and family planning services.

// COMMAND ----------

// DBTITLE 1,G-7 Country Birth Rates 1960-2013
// MAGIC %sql
// MAGIC
// MAGIC SELECT CountryName,Year,Value FROM Indicators WHERE IndicatorCode = 'SP.DYN.CBRT.IN' AND CountryName IN ('Canada','France','Germany', 'United Kingdom', 'Italy', 'Japan','United States')

// COMMAND ----------

// MAGIC %md
// MAGIC ## What is World Per Capita Income
// MAGIC er capita income refers to the average income earned per person in a specific region, country, or the entire world. It is typically calculated by dividing the total income of a region or country by its population.

// COMMAND ----------

// DBTITLE 1,World Per Capita Income in 2013
// MAGIC %sql 
// MAGIC SELECT countrycode,int(value) FROM Indicators WHERE Year = 2013 AND IndicatorCode = 'NY.ADJ.NNTY.PC.KD' order by value desc limit 250;
