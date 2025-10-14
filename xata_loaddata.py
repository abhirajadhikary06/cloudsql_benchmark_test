import pandas as pd
import psycopg2
from psycopg2 import sql
from io import StringIO
import os
import time
import csv
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

# Start timing
start_time = time.time()

# ---- Step 1: Load and Validate CSV ----
print("Loading CSV data...")
csv_file = "uni_dataset.csv"
try:
    df = pd.read_csv(csv_file, encoding='utf-8', sep=',', quotechar='"', escapechar='\\')
except Exception as e:
    print(f"Error reading CSV file: {e}")
    sys.exit(1)
print(f"Loaded {len(df)} rows from CSV file.")

# Validate and clean CSV
expected_columns = ['name', 'uni_roll', 'branch']  # Removed phone_number
if not all(col in df.columns for col in expected_columns):
    print(f"CSV missing required columns. Expected: {expected_columns}")
    sys.exit(1)
if len(df.columns) > len(expected_columns):
    print("Warning: CSV contains extra columns. Selecting only required columns.")
    df = df[expected_columns]

# Clean data
df['name'] = df['name'].astype(str).str.replace(',', '').str.replace('"', '').str[:255]
df['uni_roll'] = df['uni_roll'].astype(str).str.replace(',', '').str.replace('"', '').str[:100]
df['branch'] = df['branch'].astype(str).str.replace(',', '').str.replace('"', '').str[:50]
df = df.dropna(subset=expected_columns)
duplicates = df[df['uni_roll'].duplicated()]
if not duplicates.empty:
    print(f"Warning: Found {len(duplicates)} duplicate uni_roll values. Removing duplicates...")
    df = df.drop_duplicates(subset=['uni_roll'], keep='first')
print(f"Final row count after cleaning: {len(df)}")

# ---- Step 2: Database connection details ----
db_url = os.getenv('XATA_DATABASE_URL') or "postgresql://6qhamt:xau_NGc5uJHt6ZUcNBChcsaGGBTJ7uSdmtDz1@ap-southeast-2.sql.xata.sh/benchmark:main?sslmode=require"

# ---- Step 3: Connect to Xata ----
try:
    conn = psycopg2.connect(
        db_url,
        application_name="bulk_loader",
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )
    conn.autocommit = True
    cur = conn.cursor()
    print("Connected to Xata successfully.")
except Exception as e:
    print(f"Database connection failed: {e}")
    sys.exit(1)

# ---- Step 4: Create the table if not exists ----
print("Creating table if it doesn't exist...")
create_table_query = """
CREATE TABLE IF NOT EXISTS students (
    name VARCHAR(255),
    uni_roll VARCHAR(100) PRIMARY KEY,
    branch VARCHAR(50)
);
"""
try:
    cur.execute(create_table_query)
    print("Table created or already exists.")
except Exception as e:
    print(f"Error creating table: {e}")
    cur.close()
    conn.close()
    sys.exit(1)

# ---- Step 5: Use COPY command for fastest data loading ----
print("Preparing data for bulk loading...")

# Create a temporary table without constraints
try:
    cur.execute("DROP TABLE IF EXISTS temp_students;")
    cur.execute("""
    CREATE TABLE temp_students (
        name VARCHAR(255),
        uni_roll VARCHAR(100),
        branch VARCHAR(50)
    );
    """)
    print("Temporary table created.")
except Exception as e:
    print(f"Error creating temporary table: {e}")
    cur.close()
    conn.close()
    sys.exit(1)

# Convert DataFrame to CSV in memory
print("Converting data for COPY operation...")
buffer = StringIO()
df.to_csv(buffer, index=False, header=False, sep=',', quoting=csv.QUOTE_MINIMAL)
buffer.seek(0)

# Use COPY command to bulk load data
try:
    print("Starting bulk data load with COPY command...")
    cur.copy_from(
        buffer,
        'temp_students',
        sep=',',
        columns=('name', 'uni_roll', 'branch')
    )
    print(f"Loaded {len(df)} rows into temporary table.")
except Exception as e:
    print(f"Error during COPY operation: {e}")
    df.to_csv('failed_rows.csv', index=False)
    print("Failed data saved to failed_rows.csv")
    cur.close()
    conn.close()
    sys.exit(1)

# Insert from temp table to main table with conflict handling
try:
    print("Moving data from temporary to main table with conflict handling...")
    cur.execute("""
    INSERT INTO students (name, uni_roll, branch)
    SELECT name, uni_roll, branch FROM temp_students
    ON CONFLICT (uni_roll) DO UPDATE 
    SET name = EXCLUDED.name,
        branch = EXCLUDED.branch;
    """)
    print("Data transferred to main table with conflict resolution.")
except Exception as e:
    print(f"Error transferring data to main table: {e}")
    cur.close()
    conn.close()
    sys.exit(1)

# Clean up temporary table
try:
    cur.execute("DROP TABLE temp_students;")
    print("Temporary table cleaned up.")
except Exception as e:
    print(f"Warning: Could not drop temporary table: {e}")

# Verify the data was loaded
try:
    cur.execute("SELECT COUNT(*) FROM students;")
    count = cur.fetchone()[0]
    print(f"Verification: {count} rows in students table.")
    if count != len(df):
        print(f"Warning: Expected {len(df)} rows, but found {count} rows.")
except Exception as e:
    print(f"Warning: Could not verify row count: {e}")

# ---- Step 6: Close the connection ----
cur.close()
conn.close()
print("Connection closed.")

# Report total time
end_time = time.time()
print(f"Total execution time: {end_time - start_time:.2f} seconds.")