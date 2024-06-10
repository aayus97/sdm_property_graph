# Graph Data Preparation and Analytics with PySpark and Neo4j

This project demonstrates a complete data pipeline for loading, transforming, and analyzing graph data using PySpark and Neo4j. The pipeline includes data extraction from Parquet files, transformation into suitable formats, loading data into Neo4j, creating relationships, performing graph analytics, and visualizing results.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Project Structure](#project-structure)
3. [Setup and Installation](#setup-and-installation)
4. [Data Pipeline Workflow](#data-pipeline-workflow)
5. [Graph Analytics](#graph-analytics)
6. [Visualizations](#visualizations)
7. [Usage](#usage)
8. [Contributing](#contributing)
9. [License](#license)

## Prerequisites

Before running the project, ensure you have the following software installed:

- Python 3.8 or higher
- Apache Spark
- Neo4j (version 4.x or higher)
- Java Development Kit (JDK) 8 or higher

## Project Structure

The project directory contains the following files:


## Setup and Installation

1. **Clone the repository**:
    ```bash
    git clone [git@github.com:aayus97/sdm_property_graph.git]
    cd graph-data-pipeline
    ```

2. **Install Python dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3. **Start Neo4j**:
    Ensure Neo4j is running and accessible at `bolt://localhost:7687` with the username `neo4j` and password ``. Modify these credentials in the `main.py` script if necessary.

## Data Pipeline Workflow

The data pipeline consists of several key steps:

1. **Initialize Spark Session**: Create a Spark session for data processing.
2. **Load Data**: Read data from Parquet files into Spark DataFrames.
3. **Transform Data**: Process and transform the data into suitable formats for graph operations.
4. **Initialize Neo4j Driver**: Set up the connection to the Neo4j database.
5. **Write Data to Neo4j**: Load nodes and relationships into Neo4j.
6. **Verify Relationships**: Check the relationships in the Neo4j database.
7. **Graph Analytics**: Perform various graph analytics operations.
8. **Visualize Results**: Generate visualizations of the graph schema and analytics results.

## Graph Analytics

The pipeline performs several graph analytics operations using Neo4j Graph Data Science library:

1. **Shortest Path**: Finds the shortest path between two customers.
2. **PageRank**: Calculates the importance of products based on customer purchases.
3. **Louvain Community Detection**: Identifies communities of customers based on purchase relationships.
4. **Node Similarity**: Finds similar nodes based on relationships.
5. **Link Prediction**: Predicts potential future links between nodes using the Adamic-Adar algorithm.

## Visualizations

The project includes functionality to visualize the graph schema and analytics results using NetworkX and Matplotlib.

## Usage

1. **Run the data pipeline**:
    ```bash
    python main.py
    ```

2. **Check the Neo4j database**:
    Verify the nodes, relationships, and results of graph analytics by accessing the Neo4j browser at `http://localhost:7474`.

3. **View visualizations**:
    The visualizations are saved as PNG files in the project directory.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request if you have any improvements or new features to propose.


