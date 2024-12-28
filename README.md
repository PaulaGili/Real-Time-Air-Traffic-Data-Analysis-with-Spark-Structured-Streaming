# Aircraft Data Streaming Analysis with Spark Structured Streaming

## Project Title: **Aircraft Data Streaming Analysis with Spark Structured Streaming**

### Description:
This project focuses on the analysis of real-time data streams using Apache Spark's Structured Streaming API. The primary goal of the project is to demonstrate how to work with streaming data, particularly from an ADS-B Exchange API, and perform data processing and analysis on flight data using Spark SQL and Spark DataFrames.

The project consists of several exercises that guide the user through:
- Setting up and consuming streaming data from a remote socket.
- Processing JSON data and performing various transformations.
- Analyzing specific subsets of data, such as filtering based on geographic locations and computing distance to airports.
- Defining and implementing KPIs relevant to the air traffic management domain.

### Exercises:
1. **Exercise 1: Streaming Data and Basic Processing**
   - Establish a connection to the flight data stream and display the JSON data in the console.
   - Create the script `ejercicio1.py` to connect to the socket and show the data.

2. **Exercise 2: Data Pre-processing**
   - Parse the incoming JSON string to a structured DataFrame.
   - Extract and transform relevant information (e.g., flight, latitude, longitude, altitude, category) into a structured DataFrame.
   - Create the script `ejercicio2.py` to preprocess and transform the data.

3. **Exercise 3: Data Filtering and Time Conversion**
   - Filter the flight data to include only flights within the Catalonia region (defined by specific latitude and longitude boundaries).
   - Add a new column with timestamps based on the `now` field in the incoming data.
   - Implement this logic in `ejercicio3.py`.

4. **Exercise 4: Distance Calculation and Aggregation**
   - Calculate the distance from each flight to various airports (Barcelona, Tarragona, Girona) using the Haversine formula.
   - Group the flights by category and display the count of flights in each category.
   - Create the script `ejercicio4.py` to perform these tasks and show the results.

5. **Exercise 5: KPI Definition**
   - Define two Key Performance Indicators (KPIs) relevant to air traffic management.
   - Include the KPI name, definition, formula (if applicable), data used, measurement units, and frequency of measurement.

6. **Exercise 6: KPI Implementation**
   - Implement the defined KPIs using Spark SQL and DataFrame operations on the processed data.

### Prerequisites:
- Python 3.x
- Apache Spark 2.4.x (or higher)
- JupyterLab or any Linux-based terminal (for SSH access)
- `pyspark` library
- `requests` library for API consumption
