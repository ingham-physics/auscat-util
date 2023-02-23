#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This module defines some functions that can be invoked 
when executing SQL queries, d2rq pipeline and Pentaho scripts.
"""
import logging
import os
import subprocess
import sys
from urllib.request import urlopen
import sqlalchemy
import pandas as pd
import numpy as np
import yaml
import psycopg2
from sqlalchemy import create_engine
from SPARQLWrapper import __agent__, SPARQLWrapper, JSON, POST, Wrapper, XML
from lxml import etree


logging.getLogger().setLevel(logging.INFO)


class SqlScriptRunner:
    def __init__(self, sqlfilename, configfile):
        self.sqlfilename = sqlfilename
        self.configfile = configfile

    def run_sql_script(self):
        """
        Run SQL scripts in the sqlfile.
        """
        logging.info("Running SQL: " + self.sqlfilename)
        # open and read SQL file
        with open(self.sqlfilename, "r") as fd:
            sqlFile = fd.read()
        sqlCommands = sqlFile.split(";")
        # convert a YAML file to a Python object
        config = get_yaml_config(self.configfile)
        # connect to PostgreSQL database
        conn = psycopg2.connect(
            host=f'{config["hostname"]}',
            port=f'{config["portnumber"]}',
            database=f'{config["dbname"]}',
            user=f'{config["dbUser"]}',
            password=f'{config["dbPass"]}',
        )
        cursor = conn.cursor()
        conn.set_isolation_level(0)
        # execute every command from the input file
        try:
            for command in sqlCommands[:-1]:
                cursor.execute(f"{command}")
                conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error("Error while creating PostgreSQL table", error)
        finally:
            if conn:
                cursor.close()
                conn.close()
                logging.info("Query/Script completed")

    def get_dataframe(self):
        """
        Run SQL scripts in sqlfile, return a dataframe.

        Returns:
            query result in the form of dataframe.
        """
        logging.info("Running SQL: " + self.sqlfilename)
        # open and read SQL file
        with open(self.sqlfilename, "r") as fd:
            sqlFile = fd.read()

        try:
            sqlCommands = sqlFile.split(";")
            # convert a YAML file to a Python object
            config = get_yaml_config(self.configfile)
            # connect to PostgreSQL database
            db_string = (
                f'postgresql://{config["dbUser"]}'
                f':{config["dbPass"]}'
                f'@{config["hostname"]}'
                f':{config["portnumber"]}'
                f'/{config["dbname"]}'
            )

            engine = create_engine(db_string)
            df = pd.read_sql_query(sqlCommands[0], engine)
            pd.set_option(
                "display.expand_frame_repr", False
            )  # option: expand output display of dataframe
            logging.info(
                "Returning Query %s results in Dataframe format...", self.sqlfilename
            )
            return df
        except Exception as e:
            logging.error("Error while returning Query results in Dataframe format.", e)
        finally:
            engine.dispose()

    def get_dataframe_in_line(self, sqlcommand):
        """
        Pass SQL commands in line.

        Args:
            sqlcommand: SQL command in line.
        Returns:
            query result in the form of dataframe.
        """
        # convert a YAML file to a Python object
        config = get_yaml_config(self.configfile)
        # connect to PostgreSQL database
        db_string = (
            f'postgresql://{config["dbUser"]}'
            f':{config["dbPass"]}'
            f'@{config["hostname"]}'
            f':{config["portnumber"]}'
            f'/{config["dbname"]}'
        )
        try:
            engine = create_engine(db_string)
            df = pd.read_sql_query(sqlcommand, engine)
            pd.set_option(
                "display.expand_frame_repr", False
            )  # option: expand output display of dataframe
            return df
        except Exception as e:
            logging.error("Error while returning Query results in Dataframe format", e)
        finally:
            engine.dispose()

    def import_csv(self, csv_path):
        """
        Run a sql file, import a csv file to postgres, generate a table.
        """
        # convert a YAML file to a Python object
        config = get_yaml_config(self.configfile)
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            host=f'{config["hostname"]}',
            port=f'{config["portnumber"]}',
            database=f'{config["dbname"]}',
            user=f'{config["dbUser"]}',
            password=f'{config["dbPass"]}',
        )
        cursor = conn.cursor()

        # Read sql command from sqlfile
        with open(self.sqlfilename, "r") as fd:
            s = fd.read()
        # Import csv file
        with open(csv_path) as file:
            cursor.copy_expert(s, file)

        try:
            logging.info(
                "Importing a csv file %s to postgres, generating a table...",
                csv_path,
            )
            conn.commit()
        except Exception as e:
            logging.error("Error while importing a csv file to postgres.", e)

    def export_to_csv(self, csv_path):
        """
        Run a sql file, export result table to a .csv file.
        """
        # convert a YAML file to a Python object
        config = get_yaml_config(self.configfile)
        # connect to PostgreSQL database
        conn = psycopg2.connect(
            host=f'{config["hostname"]}',
            port=f'{config["portnumber"]}',
            database=f'{config["dbname"]}',
            user=f'{config["dbUser"]}',
            password=f'{config["dbPass"]}',
        )
        cursor = conn.cursor()

        # Read sql command from sqlfile
        s = read_text_file(self.sqlfilename)
        # Convert to csv file
        with open(csv_path, "w") as file:
            cursor.copy_expert(s, file)
        try:
            logging.info("Exporting result table to a .csv file %s...", csv_path)
            conn.commit()
        except Exception as e:
            logging.error("Error while exporting result table to a .csv file.", e)

    def commit_dataframe(self, df, tablename, target_schema):
        """
        Load contents from a dataframe into a table in database.

        Args:
            df: dataframe name.
            tablename: name of PostgreSQL table.
            target_schema: table schema.
        """
        # convert a YAML file to a Python object
        config = get_yaml_config(self.configfile)
        # Connect to PostgreSQL database
        db_string = (
            f'postgresql://{config["dbUser"]}'
            f':{config["dbPass"]}'
            f'@{config["hostname"]}'
            f':{config["portnumber"]}'
            f'/{config["dbname"]}'
        )
        try:
            engine = create_engine(db_string)
            df.to_sql(tablename, engine, if_exists="replace", schema=target_schema)
            logging.info("Writing records stored in a DataFrame to PostgreSQL...")
        except Exception as e:
            logging.error("Error while returning Query results in Dataframe format.", e)
        finally:
            engine.dispose()

    def show_psycopg2_exception(err):
        # Define a function that handles and parses psycopg2 exceptions
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()
        # get the line number when exception occured
        line_n = traceback.tb_lineno
        # the connect() error
        logging.error("\npsycopg2 ERROR:", err, "on line number:", line_n)
        logging.error("\npsycopg2 traceback:", traceback, "-- type:", err_type)
        # psycopg2 extensions.Diagnostics object attribute
        logging.error("\nextensions.Diagnostics:", err.diag)
        # the pgcode and pgerror exceptions
        logging.error("pgerror:", err.pgerror)
        logging.error("pgcode:", err.pgcode, "\n")

    @staticmethod
    def get_yaml_config(configfile):
        """
        Load a YAML file and return a Python object.

        Args:
            configfile: in the format of YAML.
        Returns:
            contents in YAML file in the format of Python dictionary.
        """
        with open(configfile) as f:
            try:
                config = yaml.full_load(f)
                return config
            except yaml.YAMLError as e:
                logging.error(e)


class SPARQLQueryRunner:
    def __init__(self, endpoint):
        self.endpoint_location = endpoint_location
        self.endpoint_update = endpoint_update
        self.rdf_repository = rdf_repository

    def run_sparql_query(self, query):
        """
        Return Sparql query results (with headers) in DataFrame format.

        Args:
            query: SPARQL query.
        """
        sparql = SPARQLWrapper(self.endpoint_location)

        # read SPARQL query
        with open(query, "r") as fd:
            logging.info("Start reading the SPARQL query...")
            querytext = fd.read()
            sparql.setQuery(querytext)

        try:
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()

            dfvars = results["head"]["vars"]
            data = pd.DataFrame(columns=dfvars)
            for result in results["results"]["bindings"]:
                d = pd.DataFrame(columns=dfvars)
                for v in dfvars:
                    d.at[0, v] = result[v]["value"] if v in result else np.NaN
                data = pd.concat([data, d], ignore_index=True)
            # show all columns and rows in the dataframe
            pd.set_option(
                "display.max_rows",
                None,
                "display.max_columns",
                None,
                "display.max_colwidth",
                None,
            )
            logging.info(
                "Return Sparql query results (with headers) in DataFrame format successfully."
            )
        except Exception as e:
            logging.error(
                "Error while returning SPARQL Query results in Dataframe format.", e
            )

        return data

    def rdfdb_clear(self, rdfGraph=None):
        """
        Clear the repository if repository already existed.
        Args:
            rdfGraph (optional): Resource Description Framework name.
        """
        sparql = SPARQLWrapper(self.endpoint_location + "/statements")
        sparql.setMethod(POST)

        if rdfGraph == None:
            sparql.setQuery(
                """
                delete where {
                    ?s ?p ?o.
                }
                """
            )
        else:
            sparql.setQuery(
                f"""
                delete {{ graph {rdfGraph} where {{
                    ?s ?p ?o.
                }}}}
                """
            )

        try:
            logging.info("Clear the repository if repository already existed.")
            results = sparql.query()
        except Exception as e:
            logging.error("Error while clearing the repository.", e)

    def rdfdb_insert(self, triple_store_path, rdfGraph=None):
        """
        Send the mapped data to the repository.
        Args:
            triple_store_path: the path of RDF triplestore
            (a graph database that stores semantic facts).
            rdfGraph (optional): Resource Description Framework name.
        """
        # Read ttl file
        with open(triple_store_path, "r") as fd:
            ttlFile = fd.read()

        sparql = SPARQLWrapper(self.endpoint_update + "/update")
        sparql.setMethod(POST)

        if rdfGraph == None:
            sparql.setQuery(
                f"""
                INSERT DATA {{
                    {ttlFile}
                }}
                """
            )
        else:
            sparql.setQuery(
                f"""
                INSERT DATA {{ GRAPH {rdfGraph}  {{
                    {ttlFile}
                }}}}
                """
            )

        try:
            logging.info("Send the mapped data to the repository...")
            results = sparql.query()
        except Exception as e:
            logging.error("Error while sending the mapped data to the repository.", e)

    def rdfdb_create(self):
        """
        Create remote repositories on rdf4j.
        """
        temp = f"""
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
        @prefix rep: <http://www.openrdf.org/config/repository#>.
        @prefix sr: <http://www.openrdf.org/config/repository/sail#>.
        @prefix sail: <http://www.openrdf.org/config/sail#>.
        @prefix ms: <http://www.openrdf.org/config/sail/memory#>.
        [] a rep:Repository ;
        rep:repositoryID "{self.rdf_repository}" ;
        rdfs:label "{self.rdf_repository}" ;
        rep:repositoryImpl [
            rep:repositoryType "openrdf:SailRepository" ;
            sr:sailImpl [
            sail:sailType "openrdf:MemoryStore" ;
            ms:persist true ;
            ms:syncDelay 120
            ]
        ].
        """

        with open("createrepo.ttl", "w") as f:
            logging.info("Write the createrepo.ttl file.")
            f.write(temp)

        try:
            cURL_cmd = f"""curl -X PUT -H "Content-type: text/turtle" --data-binary @createrepo.ttl {self.endpoint_location}"""
            result = subprocess.Popen(
                cURL_cmd, shell=True, universal_newlines=True
            ).wait()
            logging.info("Creating remote repositories on rdf4j...")
        except Exception as e:
            logging.error("Error while creating remote repositories on rdf4j.", e)

    def sparqlQuery_return_XML(self, query):
        """
        Return Sparql query results (with headers) in XML format.

        Args:
            query: SPARQL query.
        """
        sparql = SPARQLWrapper(self.endpoint_location)

        # read SPARQL query
        with open(query, "r") as fd:
            querytext = fd.read()
            sparql.setQuery(querytext)

        try:
            sparql.setReturnFormat(XML)
            results = sparql.query().convert()
            logging.info("Return SPARQL Query results in XML format successfully.")
        except Exception as e:
            logging.error(
                "Error while returning SPARQL Query results in XML format.", e
            )

        return results.toxml()

    def sparqlQuery_return_JSON(self, query):
        """
        Return Sparql query results (with headers) in JSON format.

        Args:
            query: SPARQL query.
        """
        sparql = SPARQLWrapper(self.endpoint_location)

        # read SPARQL query
        with open(query, "r") as fd:
            querytext = fd.read()
            sparql.setQuery(querytext)

        try:
            sparql.setReturnFormat(JSON)
            request = sparql._createRequest()
            request.add_header("Accept", "application/sparql-results+json")
            response = urlopen(request)
            res = Wrapper.QueryResult((response, sparql.returnFormat))
            result = res.convert()
            logging.info("Return SPARQL Query results in JSON format.")
        except Exception as e:
            logging.error(
                "Error while returning SPARQL Query results in JSON format.", e
            )

        return result


class PentahoConnectionRemover:
    def __init__(self, pathlist):
        self.pathlist = pathlist

    def remove_connections(self):
        """For all pentaho data integration files (*.kjb, *.ktr) in pathlist remove the <connection> tag
        and the contents that are within then resave the file in the same location.
        """
        for path in self.pathlist:
            for filename in os.listdir(path):
                if filename.endswith(".ktr") or filename.endswith(".kjb"):
                    tree = etree.parse(os.path.join(path, filename))
                    etree.strip_elements(tree, "{*}connection", with_tail=True)
                    save = os.path.join(path, filename)
                    tree.write(save)
