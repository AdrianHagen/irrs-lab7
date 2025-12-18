from elasticsearch import Elasticsearch, helpers

es = Elasticsearch("http://localhost:9200")

index_name = 'news'

# Define dummy data with 'path' and 'text' fields
# Paths now have at least 2 components for compatibility with ExtractData.py
documents = [
    {"path": "politics/doc1.txt", "text": "The government announced a new budget for the upcoming year."},
    {"path": "sports/doc2.txt", "text": "Sports fans are excited about the football finals this weekend."},
    {"path": "business/doc3.txt", "text": "The stock market saw a significant rise in technology shares."},
    {"path": "politics/doc4.txt", "text": "Local elections are being held to decide the new mayor."},
    {"path": "technology/doc5.txt", "text": "A new technology startup is revolutionizing the way we use AI."},
    {"path": "weather/doc6.txt", "text": "The weather forecast predicts heavy rain and storms for the coast."},
    {"path": "politics/doc7.txt", "text": "International trade agreements were signed by the two countries."},
    {"path": "sports/doc8.txt", "text": "The championship match ended in a draw after overtime."},
    {"path": "science/doc9.txt", "text": "Scientists have discovered a new species in the deep ocean."},
    {"path": "education/doc10.txt", "text": "Education reforms are necessary to improve student performance."}
]

def generate_actions():
    for doc in documents:
        yield {
            "_index": index_name,
            "_source": doc
        }

def create_index():
    # Delete index if it exists so this can be run cleanly multiple times
    if es.indices.exists(index=index_name):
        print(f"Deleting old index '{index_name}'...")
        es.indices.delete(index=index_name)

    # Create the index
    es.indices.create(index=index_name)
    print(f"Created new index '{index_name}'...")

    # Bulk load the data
    helpers.bulk(es, generate_actions())
    print(f"Successfully indexed {len(documents)} documents.")

if __name__ == "__main__":
    try:
        create_index()
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure Elasticsearch is running on http://localhost:9200")