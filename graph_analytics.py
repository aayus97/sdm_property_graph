import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit
from neo4j import GraphDatabase
from pyspark.sql import functions as F
from tabulate import tabulate


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Neo4j configuration
uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"

# Initialize Spark session
def init_spark(app_name="Graph Data Preparation"):
    return SparkSession.builder.appName(app_name).getOrCreate()

# Load data from Parquet file
def load_data(spark, file_path):
    logging.info(f"Loading data from {file_path}")
    return spark.read.parquet(file_path)

# Data transformation
def transform_data(data_df, customer_location_df, location_df):
    logging.info("Transforming data...")

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
        .filter(col("buying_customer_id").isNotNull() & col("selling_date").isNotNull() & col("selling_customer_id").isNotNull())

    customer_location_joined_df = customer_location_df.join(
        location_df, "location_id", "inner"
    ).select(
        "customer_id", location_df["location_id"], "country_code", "postal_code", "place_name",
        "admin_name1", "admin_code1", "admin_name2", "admin_code2",
        "admin_name3", "admin_code3", "latitude", "longitude", "accuracy"
    ).distinct()

    logging.info("Data transformation complete.")
    return customers_df, products_df, transactions_df, purchased_df, consumed_df, sold_to_df, customer_location_joined_df

# Initialize Neo4j driver
def init_neo4j(uri, username, password):
    logging.info("Initializing Neo4j driver...")
    return GraphDatabase.driver(uri, auth=(username, password))

# Write data to Neo4j
def write_dataframe_to_neo4j(df, query, driver, batch_size=1000):
    def write_batch(batch):
        with driver.session() as session:
            for row in batch:
                try:
                    logging.info(f"Executing query with parameters: {row.asDict()}")
                    session.run(query, parameters=row.asDict())
                except Exception as e:
                    logging.error(f"Error: {e}")

    rows = df.collect()
    logging.info(f"Number of rows to write: {len(rows)}")
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

            located_at_query = """
            MATCH ()-[r:LOCATED_AT]->()
            RETURN COUNT(r) AS count
            """
            located_at_result = session.run(located_at_query)
            count = located_at_result.single()['count']
            logging.info(f"LOCATED_AT relationships count: {count}")

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

# Group nearest customers
def group_nearest_customers(driver):
    query = """
    MATCH (c:Customer)-[:LOCATED_AT]->(l:Location)
    WITH c, l, point({latitude: l.latitude, longitude: l.longitude}) AS c_point

    MATCH (c2:Customer)-[:LOCATED_AT]->(l2:Location)
    WHERE c <> c2
    WITH c, l, c2, l2, c_point, point({latitude: l2.latitude, longitude: l2.longitude}) AS c2_point
    WITH c, l, c2, l2, c_point, c2_point, point.distance(c_point, c2_point) AS distance
    ORDER BY distance ASC
    WITH c, l, collect({customer: c2, location: l2, distance: distance, reason: 'proximity'}) AS nearest_customers

    RETURN c.customer_id AS customer_id, 
           c.customer_name AS customer_name, 
           l.latitude AS c_latitude,
           l.longitude AS c_longitude,
           nearest_customers[0..5] AS nearest_customers  // Adjust the range based on how many nearest you want to group
    """

    with driver.session() as session:
        result = session.run(query)
        grouped_customers = result.data()
        for group in grouped_customers:
            logging.info(f"Customer ID: {group['customer_id']}, Customer Name: {group['customer_name']}, Latitude: {group['c_latitude']}, Longitude: {group['c_longitude']}")
            for neighbor in group['nearest_customers']:
                neighbor_customer = neighbor['customer']
                neighbor_location = neighbor['location']
                reason = neighbor['reason']
                logging.info(
                    f"    Neighbor ID: {neighbor_customer['customer_id']}, Latitude: {neighbor_location['latitude']}, Longitude: {neighbor_location['longitude']}, Distance: {neighbor['distance']} meters, Reason: {reason}")

# Create Location nodes
def create_location_nodes(location_list, driver):
    location_query = """
    UNWIND $rows AS row
    MERGE (l:Location {location_id: row.location_id})
    SET l.country_code = row.country_code, 
        l.postal_code = row.postal_code, 
        l.place_name = row.place_name, 
        l.latitude = row.latitude, 
        l.longitude = row.longitude, 
        l.accuracy = row.accuracy
    """
    with driver.session() as session:
        session.run(location_query, parameters={"rows": location_list})



def create_bought_together_relationships(data_df, driver):
    # Self-join to create pairs of products bought by the same customer
    bought_together_df = data_df.alias("df1").join(
        data_df.alias("df2"),
        (F.col("df1.customer_id") == F.col("df2.customer_id")) &
        (F.col("df1.purchase_date") == F.col("df2.purchase_date")) &
        (F.col("df1.product_name") < F.col("df2.product_name"))
    ).select(
        F.col("df1.product_name").alias("product_id_1"),
        F.col("df2.product_name").alias("product_id_2"),
        F.col("df1.purchase_date"),
        F.col("df1.customer_id")
    )

    # Count occurrences of each product pair
    bought_together_counts_df = bought_together_df.groupBy("product_id_1", "product_id_2").count()

    # Select top 5 most frequently bought together pairs
    top_bought_together_df = bought_together_counts_df.orderBy(F.desc("count")).limit(5)

    # Join with original DataFrame to get customer_id and purchase_date for the top pairs
    top_pairs_with_details_df = top_bought_together_df.join(
        bought_together_df,
        ["product_id_1", "product_id_2"]
    ).select("customer_id", "product_id_1", "product_id_2", "purchase_date")

    top_bought_together_list = df_to_dict_list(top_pairs_with_details_df)
    logging.info("Top pairs of products bought together:")
    logging.info(tabulate(top_bought_together_list, headers="keys", tablefmt="grid"))

    query = """
    UNWIND $rows AS row
    MATCH (c:Customer {customer_id: row.customer_id})
    MATCH (p1:Product {product_name: row.product_id_1})
    MATCH (p2:Product {product_name: row.product_id_2})
    MERGE (p1)-[r:BOUGHT_TOGETHER {customer_id: row.customer_id, purchase_date: row.purchase_date}]->(p2)
    """

    with driver.session() as session:
        session.run(query, parameters={"rows": top_bought_together_list})


def query_products_bought_together_by_category(driver):
    query = """
    MATCH (p1:Product)-[r:BOUGHT_TOGETHER]->(p2:Product)
    WHERE p1.category = p2.category
    RETURN p1.product_id AS product1, p2.product_id AS product2, p1.category AS category, COUNT(r) AS bought_together_count
    ORDER BY bought_together_count DESC
    LIMIT 5
    """
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            logging.info(
                f"Product1: {record['product1']}, Product2: {record['product2']}, Category: {record['category']}, Bought Together Count: {record['bought_together_count']}"
            )



# Create LOCATED_AT relationships
def create_located_at_relationships(located_at_list, driver):
    located_at_query = """
    UNWIND $rows AS row
    MATCH (c:Customer {customer_id: row.customer_id})
    MATCH (l:Location {location_id: row.location_id})
    MERGE (c)-[r:LOCATED_AT]->(l)
    """
    with driver.session() as session:
        session.run(located_at_query, parameters={"rows": located_at_list})


def visualize_schema(driver):
    with driver.session() as session:
        # Schema Visualization
        schema_vis_query = "CALL db.schema.visualization()"
        vis_result = session.run(schema_vis_query)

        logging.info("Schema Visualization:")
        for record in vis_result:
            logging.info(record)

        # Schema Description
        schema_desc_query = "CALL db.schema.nodeTypeProperties()"
        desc_result = session.run(schema_desc_query)

        logging.info("Schema Description:")
        for record in desc_result:
            logging.info(record)

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

        # Link Prediction
        try:
            logging.info("Creating graph projection for Link Prediction...")
            with driver.session() as session:
                # Drop existing graph projection if it exists
                drop_existing_graph_projection('customerProductGraph', driver)

                # Create a new graph projection
                projection_query = """
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
                session.run(projection_query)

                # Run Adamic-Adar Link Prediction algorithm
                logging.info("Running Adamic-Adar Link Prediction...")
                adamic_adar_query = """
                    CALL gds.alpha.linkprediction.adamicAdar.stream('customerProductGraph')
                    YIELD node1, node2, score
                    RETURN 
                        CASE WHEN 'Customer' IN labels(gds.util.asNode(node1)) THEN gds.util.asNode(node1).customer_id ELSE gds.util.asNode(node1).product_id END AS Node1,
                        CASE WHEN 'Customer' IN labels(gds.util.asNode(node2)) THEN gds.util.asNode(node2).customer_id ELSE gds.util.asNode(node2).product_id END AS Node2,
                        score
                    ORDER BY score DESC
                    LIMIT 10
                    """
                result = session.run(adamic_adar_query)
                predictions = [{"Node1": record["Node1"], "Node2": record["Node2"], "Score": record["score"]} for record
                               in result]

                logging.info("Link Prediction (Adamic-Adar) Results:")
                logging.info(tabulate(predictions, headers="keys", tablefmt="grid"))
        except Exception as e:
            logging.error(f"Error running Link Prediction: {e}")

# Main function to run the data pipeline
def main():
    logging.info("Starting the data pipeline...")

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


    data_df.show()
    products_df.show()
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

    sold_to_list = df_to_dict_list(sold_to_df)
    create_sold_to_relationships(sold_to_list, driver)

    # Create Location nodes
    location_list = df_to_dict_list(location_df)
    create_location_nodes(location_list, driver)

    # Create LOCATED_AT relationships
    located_at_list = df_to_dict_list(customer_location_joined_df)
    create_located_at_relationships(located_at_list, driver)

    # Verify relationships before running graph analytics
    verify_relationships(driver)

    # Perform graph analytics
    perform_graph_analytics(driver)

    # Group nearest customers based on location
    group_nearest_customers(driver)

    # Create BOUGHT_TOGETHER relationships
    create_bought_together_relationships(data_df, driver)

    # Query products bought together by category
    query_products_bought_together_by_category(driver)

    # Visualize schema
    visualize_schema(driver)

    spark.stop()
    driver.close()

    logging.info("Data pipeline completed.")

if __name__ == "__main__":
    main()
