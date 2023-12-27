# Formula_1_Racing_Using_Azure_Databricks

## Project Overview:
The goal of this project is to create a data analysis system for Formula-1 race results using Azure Databricks. This involves building an ETL (Extract, Transform, Load) pipeline to collect Formula 1 racing data from ergast.com, a website focused on Formula 1 statistics, and then processing and storing it in Azure Data Lake Gen2 storage. Azure Databricks is used for data transformation and analysis, and Azure Data Factory orchestrates the entire process.

## Formula1 Overview
Formula 1 (F1) is the highest level of single-seater auto racing worldwide and is governed by the FIA. It showcases advanced and powerful cars equipped with hybrid engines. The F1 season occurs once a year and consists of races that take place over the course of a weekend, typically from Friday to Sunday. These races are held at various circuits in different countries. In each race, there are 10 teams, each with two assigned drivers.

The F1 season typically includes 20 to 23 races, also known as Grands Prix. Safety is a paramount concern, with strict regulations and ongoing technological advancements to ensure the well-being of drivers and spectators. Pit stops are a common occurrence during races, allowing teams to change tires and make adjustments to the cars.

On Saturdays, a qualifying round is held to determine the starting positions of drivers on the grid for the Sunday race. The races themselves consist of a variable number of laps, usually ranging from 50 to 70 laps, depending on the circuit's length. Pit stops are available during races for tire changes or car adjustments.

The results of each race are used to calculate driver standings and constructor standings. The driver who leads the driver standings at the end of the season is crowned the drivers' champion, while the team that leads the constructor standings becomes the constructors' champion.

## Solution Architecture for the Problem Statement

![ov4](https://github.com/hbuddana/Formula_1_Racing_Using_Azure_Databricks/assets/65592890/ae620fcc-5b65-4c4b-8508-8aca16870026)

## ER Diagram
The structure of the database is shown in the following ER Diagram and explained in the [Database User Guide](http://ergast.com/docs/f1db_user_guide.txt)
![ER](https://github.com/hbuddana/Formula_1_Racing_Using_Azure_Databricks/assets/65592890/84074fe4-1916-47c3-944e-8b414541cc8c)

## Working Plan

### Source Data

We are referring to open-source data from the website Ergast Developer API. Data was available from 1950 till 2022.

| File Name     | File Type                |
|---------------|--------------------------|
| Circuits      | CSV                      |
| Races         | CSV                      |
| Constructors  | Single Line JSON         |
| Drivers       | Single Line Nested JSON  |
| Results       | Single Line JSON         |
| PitStops      | Multi Line JSON          |
| LapTimes      | Split CSV Files          |
| Qualifying    | Split Multi Line JSON Files |



### Execution Overview:

- Azure Data Factory (ADF) plays a crucial role in managing the execution and monitoring of Azure Databricks notebooks. Our workflow involves importing data from the Ergast API and storing it in Azure Data Lake Storage Gen2 (ADLS). Initially, the raw data is placed in the Bronze zone, which serves as a landing zone.

- To process the data in the Bronze zone, we use an Azure Databricks notebook. This notebook is responsible for transforming the data into delta tables using an upsert operation. Once this transformation is complete, ADF takes charge of moving the processed data to the ADLS Silver zone, which functions as a standardization zone.

- In the Silver zone, the ingested data undergoes further transformation through an Azure Databricks SQL notebook. This involves operations such as joining and aggregating tables to prepare the data for analytical and visualization purposes. Ultimately, the results of these transformations are loaded into the Gold zone, which serves as the analytical zone for further analysis and reporting.

###  ETL pipeline:
ETL flow comprises two parts:

- Ingestion: Process data from Bronze zone to Silver zone
- Transformation: Process data from Silver zone to Gold zone

In the first pipeline, data stored in JSON and CSV format is read using Apache Spark with minimal transformation saved into a delta table. The transformation includes dropping columns, renaming headers, applying schema, and adding audited columns (ingestion_date and file_source) and file_date as the notebook parameter. This serves as a dynamic expression in ADF.

In the second pipeline, Databricks SQL reads preprocessed delta files and transforms them into the final dimensional model tables in delta format. Transformations performed include dropping duplicates, joining tables using join, and aggregating using a window.

ADF is scheduled to run every Sunday at 10 PM and is designed to skip the execution if there is no race that week. We have another pipeline to execute the ingestion pipeline and transformation pipeline using file_date as the parameter for the tumbling window trigger.

![adf](https://github.com/hbuddana/Formula_1_Racing_Using_Azure_Databricks/assets/65592890/a927c96d-1417-4678-a203-9dfdb993c324)









