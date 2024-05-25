import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit
from neo4j import GraphDatabase

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Neo4j configuration
uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"


# Initialize Spark session
def init_spark():
    return SparkSession.builder.appName("Graph Data Preparation").getOrCreate()


# Load data from Parquet file
def load_data(spark, file_path):
    return spark.read.parquet(file_path)


# Data transformation
def transform_data(data_df, customer_location_df, location_df):
    customers_df = data_df.select("customer_id", "customer_name", "email_id").distinct()

    products_df = data_df.select(
        "product_name", "unit_price", "product_in_avg_expiry_file", "avg_expiry_days"
    ).withColumnRenamed("product_name", "product_id") \
        .withColumnRenamed("product_in_avg_expiry_file", "category").distinct()

    transactions_df = data_df.withColumn(
        "transaction_id", concat(col("customer_id"), lit('_'), col("product_name"), lit('_'), col("purchase_date"))
    ).select("transaction_id", "purchase_date", "expected_expiry_date", "expiry_date", "expected_price").distinct()

    purchased_df = data_df.withColumnRenamed("product_name", "product_id") \
        .select("customer_id", "product_id", "purchase_date", "quantity", "unit_price")

    consumed_df = data_df.withColumn(
        "transaction_id", concat(col("customer_id"), lit('_'), col("product_name"), lit('_'), col("purchase_date"))
    ).withColumnRenamed("product_name", "product_id") \
        .select("product_id", "transaction_id", "percentage_consumed")

    sold_to_df = data_df.select("customer_id", "buying_customer_id", "selling_date") \
        .withColumnRenamed("customer_id", "selling_customer_id") \
        .filter(col("buying_customer_id").isNotNull() & col("selling_date").isNotNull() & col(
        "selling_customer_id").isNotNull())

    # New transformation steps for customer_location and location
    customer_location_joined_df = customer_location_df.join(
        location_df, "location_id", "inner"
    ).select(
        "customer_id", location_df["location_id"], "country_code", "postal_code", "place_name"
    ).distinct()

    return customers_df, products_df, transactions_df, purchased_df, consumed_df, sold_to_df, customer_location_joined_df


# Initialize Neo4j driver
def init_neo4j(uri, username, password):
    return GraphDatabase.driver(uri, auth=(username, password))


# Write data to Neo4j
def write_dataframe_to_neo4j(df, query, driver, batch_size=1000):
    def write_batch(batch):
        with driver.session() as session:
            for row in batch:
                try:
                    session.run(query, parameters=row.asDict())
                except Exception as e:
                    logging.error(f"Error: {e}")

    rows = df.collect()
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]
    for batch in batches:
        write_batch(batch)


# Verify relationships
def verify_relationships(driver):
    with driver.session() as session:
        try:
            sold_to_query = """
            MATCH ()-[r:SOLD_TO]->()
            RETURN COUNT(r) AS count
            """
            sold_to_result = session.run(sold_to_query)
            count = sold_to_result.single()['count']
            logging.info(f"SOLD_TO relationships count: {count}")
        except Exception as e:
            logging.error(f"Error verifying relationships: {e}")


# Drop existing graph projections
def drop_existing_graph_projection(graph_name, driver):
    with driver.session() as session:
        try:
            drop_query = f"CALL gds.graph.drop('{graph_name}', false)"
            session.run(drop_query)
            logging.info(f"Dropped existing graph projection: {graph_name}")
        except Exception as e:
            logging.warning(f"No existing graph projection named {graph_name} to drop")


# Convert DataFrame to List of Dictionaries
def df_to_dict_list(df):
    return [row.asDict() for row in df.collect()]


# Create SOLD_TO relationships
def create_sold_to_relationships(sold_to_list, driver):
    sold_to_query = """
    UNWIND $rows AS row
    MERGE (c1:Customer {customer_id: row.selling_customer_id})
    MERGE (c2:Customer {customer_id: row.buying_customer_id})
    MERGE (c1)-[r:SOLD_TO {selling_date: row.selling_date}]->(c2)
    """
    with driver.session() as session:
        session.run(sold_to_query, parameters={"rows": sold_to_list})


# Perform graph analytics
def perform_graph_analytics(driver):
    with driver.session() as session:
        # Pathfinding: Shortest Path
        try:
            logging.info("Running shortest path query...")
            pathfinding_query = """
            MATCH (c1:Customer {customer_id: '232'}), (c2:Customer {customer_id: '233'}),
            path = shortestPath((c1)-[*]-(c2))
            RETURN path
            """
            path_result = session.run(pathfinding_query)
            for record in path_result:
                logging.info(f"Shortest path: {record['path']}")
        except Exception as e:
            logging.error(f"Error running shortest path query: {e}")

        # Centrality Analysis: PageRank
        try:
            logging.info("Creating graph projection for PageRank...")
            drop_existing_graph_projection('productsGraph', driver)

            pagerank_create_query = """
            CALL gds.graph.project(
                'productsGraph',
                ['Product'],
                {
                    PURCHASED: {
                        type: 'PURCHASED',
                        orientation: 'UNDIRECTED'
                    }
                }
            )
            """
            session.run(pagerank_create_query)

            logging.info("Running PageRank...")
            pagerank_query = """
            CALL gds.pageRank.stream('productsGraph')
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).product_id AS product, score
            ORDER BY score DESC
            LIMIT 10
            """
            pagerank_result = session.run(pagerank_query)
            for record in pagerank_result:
                logging.info(f"Product: {record['product']}, PageRank Score: {record['score']}")
        except Exception as e:
            logging.error(f"Error running PageRank: {e}")

        # Community Detection: Louvain
        try:
            logging.info("Creating graph projection for Louvain...")
            drop_existing_graph_projection('customerGraph', driver)

            louvain_create_query = """
            CALL gds.graph.project(
                'customerGraph',
                ['Customer'],
                {
                    SOLD_TO: {
                        type: 'SOLD_TO',
                        orientation: 'UNDIRECTED'
                    }
                }
            )
            """
            session.run(louvain_create_query)

            logging.info("Running Louvain community detection...")
            louvain_query = """
            CALL gds.louvain.stream('customerGraph')
            YIELD nodeId, communityId
            RETURN gds.util.asNode(nodeId).customer_id AS customer, communityId
            ORDER BY communityId
            """
            louvain_result = session.run(louvain_query)
            for record in louvain_result:
                logging.info(f"Customer: {record['customer']}, Community ID: {record['communityId']}")
        except Exception as e:
            logging.error(f"Error running Louvain community detection: {e}")

        # Node Similarity
        try:
            logging.info("Creating graph projection for Node Similarity...")
            drop_existing_graph_projection('customerProductGraph', driver)

            node_similarity_create_query = """
                CALL gds.graph.project(
                    'customerProductGraph',
                    ['Customer', 'Product'],
                    {
                        PURCHASED: {
                            type: 'PURCHASED',
                            orientation: 'UNDIRECTED'
                        }
                    }
                )
                """
            session.run(node_similarity_create_query)

            logging.info("Running Node Similarity...")
            node_similarity_query = """
                CALL gds.nodeSimilarity.stream('customerProductGraph')
                YIELD node1, node2, similarity
                RETURN 
                    CASE WHEN 'Customer' IN labels(gds.util.asNode(node1)) THEN gds.util.asNode(node1).customer_name ELSE gds.util.asNode(node1).product_id END AS Node1,
                    CASE WHEN 'Customer' IN labels(gds.util.asNode(node2)) THEN gds.util.asNode(node2).customer_name ELSE gds.util.asNode(node2).product_id END AS Node2,
                    similarity
                ORDER BY similarity DESC
                LIMIT 10
                """
            node_similarity_result = session.run(node_similarity_query)
            for record in node_similarity_result:
                logging.info(f"Node1: {record['Node1']}, Node2: {record['Node2']}, Similarity: {record['similarity']}")
        except Exception as e:
            logging.error(f"Error running Node Similarity: {e}")


# Main function to run the data pipeline
def main():
    spark = init_spark()

    # Replace with the path to your Parquet files
    file_path = "platform_customer_pricing_data_output"
    customer_location_path = "data_for_fact/customer_location"
    location_path = "data_for_fact/location"

    # Load the data
    data_df = load_data(spark, file_path)
    customer_location_df = load_data(spark, customer_location_path)
    location_df = load_data(spark, location_path)

    # Transform the data
    customers_df, products_df, transactions_df, purchased_df, consumed_df, sold_to_df, customer_location_joined_df = transform_data(
        data_df, customer_location_df, location_df
    )

    driver = init_neo4j(uri, username, password)

    # Neo4j queries
    customer_query = """
    MERGE (c:Customer {customer_id: $customer_id})
    SET c.customer_name = $customer_name, c.email_id = $email_id
    """
    write_dataframe_to_neo4j(customers_df, customer_query, driver)

    product_query = """
    MERGE (p:Product {product_id: $product_id})
    SET p.unit_price = $unit_price, p.category = $category, p.avg_expiry_days = $avg_expiry_days
    """
    write_dataframe_to_neo4j(products_df, product_query, driver)

    transaction_query = """
    MERGE (t:Transaction {transaction_id: $transaction_id})
    SET t.purchase_date = $purchase_date, t.expected_expiry_date = $expected_expiry_date, t.expiry_date = $expiry_date, t.expected_price = $expected_price
    """
    write_dataframe_to_neo4j(transactions_df, transaction_query, driver)

    purchased_query = """
    MATCH (c:Customer {customer_id: $customer_id})
    MATCH (p:Product {product_id: $product_id})
    MERGE (c)-[r:PURCHASED {purchase_date: $purchase_date}]->(p)
    SET r.quantity = $quantity, r.unit_price = $unit_price
    """
    write_dataframe_to_neo4j(purchased_df, purchased_query, driver)

    consumed_query = """
    MATCH (p:Product {product_id: $product_id})
    MATCH (t:Transaction {transaction_id: $transaction_id})
    MERGE (p)-[r:CONSUMED]->(t)
    SET r.percentage_consumed = $percentage_consumed
    """
    write_dataframe_to_neo4j(consumed_df, consumed_query, driver)

    sold_to_query = """
    MATCH (c1:Customer {customer_id: $selling_customer_id})
    MATCH (c2:Customer {customer_id: $buying_customer_id})
    MERGE (c1)-[r:SOLD_TO {selling_date: $selling_date}]->(c2)
    """
    # write_dataframe_to_neo4j(sold_to_df, sold_to_query, driver)
    sold_to_list = df_to_dict_list(sold_to_df)
    create_sold_to_relationships(sold_to_list, driver)

    # Write customer_location relationships to Neo4j
    customer_location_query = """
    MATCH (c:Customer {customer_id: $customer_id})
    MATCH (l:Location {location_id: $location_id})
    MERGE (c)-[r:LOCATED_AT]->(l)
    SET l.country_code = $country_code, l.postal_code = $postal_code, l.place_name = $place_name
    """
    write_dataframe_to_neo4j(customer_location_joined_df, customer_location_query, driver)

    # Verify relationships before running graph analytics
    verify_relationships(driver)

    # Perform graph analytics
    perform_graph_analytics(driver)

    spark.stop()
    driver.close()


if __name__ == "__main__":
    main()
