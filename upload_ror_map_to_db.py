import redshift_connector
import json
from tqdm import tqdm

with open("credentials.json", 'r') as f:
    credentials = json.load(f)

# Establish a connection to your database
with redshift_connector.connect(
    database='dev',
    host='scigami-redshift-serverless-workgroup.670332334218.us-east-1.redshift-serverless.amazonaws.com',
    port='5439',  # Default redshift port
    user=credentials['user'],
    password=credentials['password']
) as conn:

    with conn.cursor() as cursor:

        # # Define the CREATE TABLE statement
        # create_table_query = """
        #     CREATE TABLE clean.ror_map (
        #         uid VARCHAR(64) NOT NULL,
        #         affiliation VARCHAR(256) NOT NULL
        #     )
        # """
        #
        # # Execute the CREATE TABLE statement
        # cursor.execute(create_table_query)

        # Define your dictionary
        with open("ror_map.json", 'r') as f:
            ror_mapper = json.load(f)

        # Prepare and execute the INSERT INTO statements
        for key, value in tqdm(ror_mapper.items()):
            if value is None:
                print(key)
            else:
                insert_query = "INSERT INTO clean.ror_map (uid, affiliation) VALUES (%s, %s)"
                cursor.execute(insert_query, (key, value.replace('\\"', '').replace('\\\\', ' ')
                                              .replace('"', '').replace('\\', ' ')))

        # Commit the transaction
        conn.commit()
