import sql as sql
from flask import Flask, request, jsonify
import pg8000

app = Flask(__name__)

# Replace with your database credentials and hostname
db_connection = pg8000.connect(
    database='GGPE',
    user='vmens',
    password='LatelySick3',
    host='excale.ddns.net',
    port='54321'
)

# Create a table to store player scores
with db_connection.cursor() as cursor:
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS RUNS (
            PLAYER_NAME VARCHAR(255),
            RUN_START TIMESTAMP,
            RUN_END TIMESTAMP,
            SCORE BIGINT,
            PRIMARY KEY(PLAYER_NAME, RUN_START)
        );
        CREATE TABLE IF NOT EXISTS INPUTS (
            player_name VARCHAR(255),
            run_start TIMESTAMP,
            fixed_frame BIGINT,
            raycast_0 FLOAT,
            raycast_30 FLOAT,
            raycast_45 FLOAT,
            raycast_315 FLOAT,
            raycast_330 FLOAT,
            collect_angle FLOAT,
            collect_length FLOAT,
            gravity_dir FLOAT,
            on_ground_top BOOLEAN,
            on_ground_bot BOOLEAN,
            switch_gravity BOOLEAN,
            PRIMARY KEY (player_name, run_start, fixed_frame)
        );
        CREATE TABLE IF NOT EXISTS MODELS (
            PLAYER_NAME VARCHAR(255) PRIMARY KEY,
            TRAIN_START TIMESTAMP,
            TRAIN_END TIMESTAMP,
            PARAMETERS BYTEA
        );
    ''')
db_connection.commit()

def execute_query(query, values=None):
    with db_connection.cursor() as cursor:
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        result = cursor.fetchall()
    db_connection.commit()
    return result

@app.route('/add_run', methods=['POST'])
def add_run():
    data = request.json
    run_data = data.get('run_data')  # Data for the RUNS table
    input_data_list = data.get('input_data')  # List of input data

    if run_data is None or input_data_list is None:
        return jsonify({'error': 'Missing run_data or input_data'}), 400

    # Insert data into the RUNS table
    run_query = sql.SQL('''
        INSERT INTO RUNS (PLAYER_NAME, RUN_START, RUN_END, SCORE)
        VALUES ({}, {}, {}, {});
    ''').format(
        sql.Literal(run_data['player_name']),
        sql.Literal(run_data['run_start']),
        sql.Literal(run_data['run_end']),
        sql.Literal(run_data['score'])
    )
    execute_query(run_query)

    # Insert data into the INPUTS table for each input
    for input_data in input_data_list:
        input_query = sql.SQL('''
            INSERT INTO INPUTS (
                player_name, run_start, fixed_frame,
                raycast_0, raycast_30, raycast_45, raycast_315, raycast_330,
                collect_angle, collect_length, gravity_dir,
                on_ground_top, on_ground_bot, switch_gravity
            )
            VALUES (
                {}, {}, {},
                {}, {}, {}, {}, {},
                {}, {}, {},
                {}, {}, {}
            );
        ''').format(
            sql.Literal(input_data['player_name']),
            sql.Literal(input_data['run_start']),
            sql.Literal(input_data['fixed_frame']),
            sql.Literal(input_data['raycast_0']),
            sql.Literal(input_data['raycast_30']),
            sql.Literal(input_data['raycast_45']),
            sql.Literal(input_data['raycast_315']),
            sql.Literal(input_data['raycast_330']),
            sql.Literal(input_data['collect_angle']),
            sql.Literal(input_data['collect_length']),
            sql.Literal(input_data['gravity_dir']),
            sql.Literal(input_data['on_ground_top']),
            sql.Literal(input_data['on_ground_bot']),
            sql.Literal(input_data['switch_gravity'])
        )
        execute_query(input_query)

    return jsonify({'message': 'Data added successfully'}), 201

@app.route('/get_server_version', methods=['GET'])
def get_server_version():
    try:
        cursor = db_connection.cursor()
        cursor.execute('SELECT version();')
        version = cursor.fetchone()[0]
        cursor.close()
        return jsonify({'server_version': version})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/delete_run/<string:player_name>/<string:run_start>', methods=['DELETE'])
def delete_run(player_name, run_start):
    cursor = db_connection.cursor()
    try:
        cursor.execute('''DELETE FROM RUNS
                          WHERE PLAYER_NAME = %s AND RUN_START = %s''', (player_name, run_start))
        rows_affected = cursor.rowcount
        db_connection.commit()

        if rows_affected == 0:
            return jsonify({'error': 'Run not found'}), 404

        return jsonify({'message': 'Run has been deleted'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_models/<string:player_name>', methods=['GET'])
def get_models(player_name):
    cursor = db_connection.cursor()
    try:
        cursor.execute('''SELECT TRAIN_START, TRAIN_END, PARAMETERS FROM players
                          WHERE PLAYER_NAME = %s''', (player_name,))
        model_data = cursor.fetchone()

        if model_data is None:
            return jsonify({'error': 'Player not found'}), 404

        train_start, train_end, parameters = model_data
        model = {
            'train_start': train_start.strftime('%Y-%m-%d %H:%M:%S'),
            'train_end': train_end.strftime('%Y-%m-%d %H:%M:%S'),
            'parameters': parameters.decode('utf-8')  # Assuming PARAMETERS is stored as bytes (BYTEA)
        }

        return jsonify({'player_name': player_name, 'model': model}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500



if __name__ == '__GGPE_API__':
    app.run(debug=True)


