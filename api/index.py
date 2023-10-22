from flask import Flask, jsonify, request
import mysql.connector

app = Flask(__name__)

local_db_config = {
    "host": "localhost",
    "user": "your_local_user",
    "password": "your_local_password",
    "database": "your_local_db"
}

planet_scale_db_config = {
    "host": "aws.connect.psdb.cloud",
    "user": "in6bsepcpgrim7hscitx",
    "password": "pscale_pw_jIdFjYbqVvi9abaMj96gRVfjHAsnY0CyQUMJSEP22eT",
    "database": "aans"
}

@app.route('/')
def home():
    return 'From Locla!'

@app.route('/fetch-data')
def fetch_data():
    # Connect to Local MySQL
    db = mysql.connector.connect(**planet_scale_db_config)
    cursor = db.cursor()
    cursor.execute("SELECT * FROM Comment")
    records = cursor.fetchall()
    db.close()

    # Convert records to JSON or your preferred format
    data = [convert_record_to_dict(record) for record in records]
    return jsonify(data)

@app.route('/push-to-planetscale', methods=['POST'])
def push_to_planetscale():
    data = request.json
    # Connect to PlanetScale
    db = mysql.connector.connect(**planet_scale_db_config)
    cursor = db.cursor()
    
    for record in data:
        # Adjust the following SQL based on your specific requirements
        sql = "INSERT INTO your_planetscale_table (column1, column2, ...) VALUES (%s, %s, ...)"
        values = (record['column1'], record['column2'], ...) # And so on
        cursor.execute(sql, values)
    
    db.commit()
    db.close()
    return jsonify({"status": "success", "message": "Data pushed to PlanetScale successfully"})

@app.route('/sync-to-planetscale')
def sync_to_planetscale():
    # Assuming you have a last_synced timestamp stored somewhere, fetch it
    last_synced = get_last_synced_timestamp()

    # Connect to Local MySQL and fetch records modified since the last sync
    local_db = mysql.connector.connect(**local_db_config)
    local_cursor = local_db.cursor()
    local_cursor.execute("SELECT * FROM your_table WHERE last_modified > %s", (last_synced,))
    records = local_cursor.fetchall()

    # Connect to PlanetScale
    ps_db = mysql.connector.connect(**planet_scale_db_config)
    ps_cursor = ps_db.cursor()
    
    for record in records:
        # Check if record exists in PlanetScale
        ps_cursor.execute("SELECT * FROM your_planetscale_table WHERE id = %s", (record['id'],))
        existing_record = ps_cursor.fetchone()
        
        if existing_record:
            # Update existing record
            # Make sure you account for all columns in your table
            sql = "UPDATE your_planetscale_table SET column1 = %s, column2 = %s WHERE id = %s"
            values = (record['column1'], record['column2'], record['id'])
            ps_cursor.execute(sql, values)
        else:
            # Insert new record
            sql = "INSERT INTO your_planetscale_table (column1, column2, ...) VALUES (%s, %s, ...)"
            values = (record['column1'], record['column2'], ...)
            ps_cursor.execute(sql, values)

    # Commit changes to PlanetScale
    ps_db.commit()

    # Update the last_synced timestamp
    set_last_synced_timestamp(now())

    return jsonify({"status": "success", "message": "Data synchronized with PlanetScale successfully"})

def convert_record_to_dict(record):
    # Convert your record to a dictionary or any other serialization format
    pass