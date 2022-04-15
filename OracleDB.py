# Program Name	: Oracle Database API Automation
# Version       : 1.0
# Date          : 1 September 2021
# Author     	: Mohamed Abdel-Gawad Ibrahim
# Email         : muhammadabdelgawwad@gmail.com


# Pandas is an open source data analysis and manipulation package
import pandas as pd

# sqlalchemy is a popular SQL toolkit and Object Relational Mapper
from sqlalchemy import create_engine


class OracleDB:
    """A class for connecting and quering on Oracle DB.

    Args:
        DataFrame (Class): A class of various Pandas dataframe methods.
    """

    def __init__(self, username, password, hostname, port, service_name, tag):
        """Instantiate OracleDB object.

        Args:
            username (str): The username of the Oracle database.
            password (str): The password of the Oracle database.
            hostname (str): The hostname of the Oracle database.
            port (str): The port number of the Oracle database
                (Should be a string).
            service_name (str): The service name of the Oracle database.
            tag (str): A tag to mark the current object.
        """

        # create an engine to connect to Oracle database.
        self.engine = create_engine(
            "oracle+cx_oracle://{}:{}@{}:{}/?service_name={}".format(
                username, password, hostname, port, service_name
            ),
            arraysize=100000,
        )

        # Set the tag attribute
        self.tag = tag

    def set_db_query(self, query, cols_list):
        """Set the attributes: query_name, query, cols_list


        Args:
            query (str): The query to be executed
            cols_list (list): A list of the columns names of the query to be executed.
        """

        self.db_query = query
        self.cols_list = cols_list

    def exec_db_query(self):
        """Execute the query and return the proxy results

        Returns:
            [Proxy Results]: The results of executing the query.
        """

        # Connect to the OracleDB and execute the query
        with self.engine.connect() as connection:
            results_proxy = connection.execute(self.db_query)

        # Return the results from proxy
        return results_proxy

    def fetch_db_in_df(self, results_proxy):
        """Fetch the results from the results_proxy, and convert
        them into a Pandas Dataframe object in the 'df' attribute

        Args:
            results_proxy (Proxy Results): The results of executing the query.
        """

        # Fetch all the results
        results = results_proxy.fetchall()

        # Convert them to df
        self.df = pd.DataFrame(results)

        # Set the columns names based on the proxy results keys
        self.df.columns = results_proxy.keys()
        
