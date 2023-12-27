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









