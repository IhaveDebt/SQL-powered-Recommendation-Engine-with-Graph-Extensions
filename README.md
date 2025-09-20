"""
SQL-powered Recommendation Engine with Graph Extensions
Single-file program demonstrating hybrid content + graph recommendations.
"""

import psycopg
import networkx as nx

DB_URL = "postgresql://postgres:postgres@localhost:5432/recommendation_db"

def init_db():
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Users table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name TEXT
            );
            """)
            # Items table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id SERIAL PRIMARY KEY,
                name TEXT,
                category TEXT
            );
            """)
            # Interactions table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS interactions (
                user_id INT,
                item_id INT,
                rating INT,
                PRIMARY KEY (user_id, item_id)
            );
            """)

            # Sample data
            cur.execute("""
            INSERT INTO users (name)
            SELECT 'User' || i FROM generate_series(1,10) AS s(i)
            ON CONFLICT DO NOTHING;
            """)
            cur.execute("""
            INSERT INTO items (name, category)
            SELECT 'Item'||i, 'Category' || ((i%5)+1) FROM generate_series(1,20) AS s(i)
            ON CONFLICT DO NOTHING;
            """)
            cur.execute("""
            INSERT INTO interactions (user_id, item_id, rating)
            SELECT (i%10)+1, (i%20)+1, ((i*3)%5)+1
            FROM generate_series(1,50) AS s(i)
            ON CONFLICT DO NOTHING;
            """)

def build_graph():
    with psycopg.connect(DB_URL) as conn:
        G = nx.Graph()
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, item_id FROM interactions")
            for user_id, item_id in cur.fetchall():
                G.add_edge(f"user_{user_id}", f"item_{item_id}")
    return G

def recommend(user_id, top_n=5):
    G = build_graph()
    user_node = f"user_{user_id}"
    # Graph-based recommendation using neighbors of neighbors
    candidates = {}
    for neighbor in G.neighbors(user_node):
        for nn in G.neighbors(neighbor):
            if nn != user_node and nn.startswith("item_"):
                candidates[nn] = candidates.get(nn, 0) + 1
    sorted_items = sorted(candidates.items(), key=lambda x: -x[1])
    print(f"Top {top_n} recommendations for user {user_id}:")
    for item, score in sorted_items[:top_n]:
        print(f"{item} (score={score})")

if __name__ == "__main__":
    init_db()
    print("SQL-powered Recommendation Engine with Graph Extensions\n")
    while True:
        uid = input("Enter user ID for recommendations (or 'exit'): ")
        if uid.lower() == "exit":
            break
        try:
            recommend(int(uid))
        except Exception as e:
            print(f"Error: {e}")
