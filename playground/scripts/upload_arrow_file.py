import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
import datetime

import pyiceberg.catalog
import pyiceberg.catalog.rest
import pyiceberg.typedef


ENDPOINT = "https://lakekeeper-f6mi9.ondigitalocean.app"
WAREHOUSE = "dlt-warehouse-2"


if __name__ == "__main__":
    
    # instantiate the catalog
    catalog = pyiceberg.catalog.rest.RestCatalog(
        name="default",
        uri=f"{ENDPOINT}/catalog",
        warehouse=WAREHOUSE,
        #  Required Parameters if OAuth2 authentication is enabled for Lakekeeper:
        # credential="<Client-ID>:<Client-Secret>",
        # **{
        #     "oauth2-server-uri": "http://localhost:30080/realms/<keycloak realm name>/protocol/openid-connect/token"
        # },
        # Optional Parameters if OAuth2 authentication is enabled for Lakekeeper:
        # scope="<Scopes to request from the IdP, i.e. lakekeeper>",
    )
    print(catalog.list_tables("default"))

    # Print download taxi dataset
    taxi_get_response = requests.get(
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    )
    with open("yellow_tripdata_2023-01.parquet", "wb") as f:
        f.write(taxi_get_response.content)
    print("Taxi dataset written to yellow_tripdata_2023-01.parquet")

    print("Load it into your PyArrow dataframe:")

    df = pq.read_table("yellow_tripdata_2023-01.parquet")

    print("Taxi dataset loaded successfully")

    print("Create a new Iceberg table:")
    if catalog.table_exists("default.taxi_dataset"):
        print("Table taxi_dataset already exists")
        catalog.drop_table("default.taxi_dataset")
        print("Existing table dropped")

    table = catalog.create_table("default.taxi_dataset", schema=df.schema)
    print("Catalog table default.taxi_dataset created")

    print("Append the dataframe to the table:")

    table.append(df)

    print("Appended the dataframe to the table successfully")

    print(f"{len(table.scan().to_arrow())} rows have been written to the table")

    print("Now generate a tip-per-mile feature to train the model on:")

    df = df.append_column("tip_per_mile", pc.divide(df["tip_amount"], df["trip_distance"]))

    print("tip-per-mile feature generated successfully")

    print("Evolve the schema of the table with the new column:")

    with table.update_schema() as update_schema:
        update_schema.union_by_name(df.schema)

    print("Table evolved successfully")

    print("And now we can write the new dataframe to the Iceberg table:")

    table.overwrite(df)

    print("Overwrote the table successfully")

    print("And the new column is there:")
    print(table.scan().to_arrow().schema)

    print(
        f"And we can see that {len(table.scan(row_filter='tip_per_mile > 0').to_arrow())}"
        "rows have tip-per-mile."
    )

    print("Let's drop the table and demonstrate the location option")

    catalog.drop_table("default.taxi_dataset")

    print("Catalog table default.taxi_dataset dropped")

    current_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H_%M_%S")
    new_location = f"s3://demo/new_location/{current_timestamp}"

    # print(f"Create the table with the location: {new_location}")

    # if catalog.table_exists("default.taxi_dataset_new_location"):
    #     print("Table taxi_dataset_new_location already exists")
    #     catalog.drop_table("default.taxi_dataset_new_location")
    #     print("Existing table dropped")

    # catalog.create_table(
    #     identifier="default.taxi_dataset_new_location",
    #     schema=df.schema,
    #     location=new_location,
    # )

    # print("Catalog table default.taxi_dataset_new_location created successfully")

