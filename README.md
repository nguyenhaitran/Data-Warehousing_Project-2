# Data-Warehousing_Project-2

## Outline of the project
In Project 1, the data warehousing method was performed to answer business queries related to crimes. Continuing from that, this project will focus on performing graph databases on the same data using Neo4J software. This project was done in group of 2. There are 5 required questions needed to be answered and students had to provide 2 more additional questions.

## Submission file included:
- YouTube video explained about our project: https://youtu.be/bkBT2dHw81E
- Cleaned data files including both node files and relationship files.
- Data cleaning/ETL process file
- Cypher script to run on Neo4J saved as .txt file
- PDF to explain the process, the result and discussion about the graph database

## Business queries
- How many crimes are recorded for a given crime type in a specified neighbourhood for a particular period?
- Find the neighbourhoods that share the same crime types, organise in decending order of the number of common crime types.
- Return the top 5 neighbourhoods for a specified crime for a specified duration.
- Find the types of crimes for each property type.
- Which month of a specified year has the highest crime rate? Return one record each for each beat.
- What is the most common crime severity (High, Medium, Low) for each zone? (additional question)
- Which crime nature (Property, Violent) has a higher number of records for each property for each quarter in a specific year? (additional question)

## Graph database implementation steps:
- Consider which attribute should be put as a node or a relationship
- Visualise the graph database by using the Arrows.app
- Do data cleaning and extract the data into nodes and relationship
- Write Cypher code to import the data into Neo4J
- Write Cypher queries to answer business questions.
