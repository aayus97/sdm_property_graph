## Graph Data Preparation and Analytics with PySpark and Neo4j
This project leverages graph-based solutions to enhance data management and analytics within our Big Data Management (BDM) system. By representing our data as a graph, we can efficiently explore complex relationships, conduct advanced analytics, and derive insights that are difficult to achieve with traditional relational databases. This report outlines the methodology, implementation, and benefits of employing graph-based solutions, culminating in a proof of concept (PoC) to demonstrate the effectiveness of the process.


### Prequisite
Before running the project, ensure you have the following software installed:

Python 3.8 or higher
Apache Spark
Neo4j (version 4.x or higher)
Java Development Kit (JDK) 8 or higher


### Project Structure
.
├── data_for_fact/
│   ├── customer_location.parquet
│   ├── location.parquet
│   └── platform_customer_pricing_data_output.parquet
├── requirements.txt
├── main.py
└── README.md


### Setup and Installation

#### 1. Clone the repository
git clone https://github.com/yourusername/graph-data-pipeline.git
cd graph-data-pipeline


#### 2. Install python dependencies
pip install -r requirements.txt

#### 3. Start Neo4j:
Ensure Neo4j is running and accessible at bolt://localhost:7687 with the username neo4j and password ''. Modify these credentials in the main.py script if necessary.



### Data Pipeline Workflow

The data pipeline consists of several key steps:

Initialize Spark Session: Create a Spark session for data processing.
Load Data: Read data from Parquet files into Spark DataFrames.
Transform Data: Process and transform the data into suitable formats for graph operations.
Initialize Neo4j Driver: Set up the connection to the Neo4j database.
Write Data to Neo4j: Load nodes and relationships into Neo4j.
Verify Relationships: Check the relationships in the Neo4j database.
Graph Analytics: Perform various graph analytics operations.
Visualize Results: Generate visualizations of the graph schema and analytics results.



