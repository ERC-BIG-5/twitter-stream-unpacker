import sqlalchemy

from src.consts import CONFIG
from sqlalchemy_utils import database_exists, create_database


def create_pg_db(db_name: str):
    pwd = CONFIG.PG_PASSWORD.get_secret_value()
    # default_connection_string = f"postgresql+psycopg2://postgres:{pwd}@localhost/postgres"

    # Define the connection string for the database we want to create
    new_db_connection_string = f"postgresql+psycopg2://postgres:{pwd}@localhost/{db_name}"

    # Create the new database
    if not database_exists(new_db_connection_string):
        create_database(new_db_connection_string)
        print(f"Database '{db_name}' created successfully.")
    else:
        print(f"Database '{db_name}' already exists.")

    # Connect to the new database
    new_engine = sqlalchemy.create_engine(new_db_connection_string)

    # Test the connection
    with new_engine.connect() as conn:
        result = conn.execute(sqlalchemy.text("SELECT 1"))
        print(f"Connected to '{db_name}' successfully.")


def get_engine(username: str, password: str, db_name: str) -> sqlalchemy.Engine:
    return sqlalchemy.create_engine(f"postgresql+psycopg2://{username}:{password}@localhost/{db_name}")


def create_user_grant_access(username: str, password: str, database_name: str):
    # SQL commands
    sql_commands = f"""
    CREATE USER {username} WITH PASSWORD '{password}';
    GRANT CONNECT ON DATABASE {database_name} TO {username};
    GRANT USAGE ON SCHEMA public TO {username};
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {username};
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {username};
    ALTER DATABASE {database_name} OWNER TO {username};
    GRANT CREATE ON SCHEMA public TO {username};
    """

    pwd = CONFIG.PG_PASSWORD.get_secret_value()
    # Execute the SQL commands
    with get_engine("postgres", pwd, "postgres").connect() as conn:

        conn.execute(sqlalchemy.text(f"DROP USER IF EXISTS {username}"))

        conn.execute(sqlalchemy.text("COMMIT"))  # Ensure we're not in a transaction
        for command in sql_commands.split(';'):
            if command.strip():
                conn.execute(sqlalchemy.text(command))
        conn.execute(sqlalchemy.text("COMMIT"))

    print("User created and permissions granted successfully.")


if __name__ == "__main__":
    create_pg_db("labelstudio")
    create_user_grant_access("labelstudio_admin", "|/Q[mn.73*H6)-Ec", "labelstudio")
