#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This module defines some functions that can be invoked 
when executing SQL queries and d2rq pipeline.
"""
import logging
import os
import subprocess
import sys
import sqlalchemy
import pandas as pd
import numpy as np
import yaml
import psycopg2
from urllib.request import urlopen
from sqlalchemy import create_engine
from SPARQLWrapper import __agent__, SPARQLWrapper, JSON, POST, Wrapper, XML

logging.getLogger().setLevel(logging.INFO)


def get_yaml_config(configfile):
    """
    Parse a YAML file in Python.

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


def run_sql_script(sqlfilename, configfile):
    """
    Run SQL scripts in the sqlfile.

    Args:
        sqlfilename: SQL scripts.
        configfile: database configuration.
    """
    logging.info("Running SQL: " + sqlfilename)
    # Open and read SQL file
    with open(sqlfilename, "r") as fd:
        sqlFile = fd.read()
    sqlCommands = sqlFile.split(";")
    # convert a YAML file to a Python object
    config = get_yaml_config(configfile)
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=f'{config["hostname"]}',
        port=f'{config["portnumber"]}',
        database=f'{config["dbname"]}',
        user=f'{config["dbUser"]}',
        password=f'{config["dbPass"]}',
    )
    cursor = conn.cursor()
    conn.set_isolation_level(0)
    # Execute every command from the input file
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


def get_dataframe(sqlfilename, configfile):
    """
    Run SQL scripts in sqlfile, return a dataframe.

    Args:
        sqlfilename: SQL scripts.
        configfile: database configuration.
    Returns:
        query result in the form of dataframe.
    """
    logging.info("Running SQL: " + sqlfilename)
    # Open and read SQL file
    with open(sqlfilename, "r") as fd:
        sqlFile = fd.read()

    try:
        sqlCommands = sqlFile.split(";")
        # convert a YAML file to a Python object
        config = get_yaml_config(configfile)
        # Connect to PostgreSQL database
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
            "Returning Query %s results in Dataframe format...", sqlfilename
        )
        return df
    except Exception as e:
        logging.error(
            "Error while returning Query results in Dataframe format.", e
        )
    finally:
        engine.dispose()


def get_dataframe_in_line(sqlCommands, configfile):
    """
    Pass SQL commands in line.

    Args:
        sqlfilename: SQL commands in line.
        configfile: database configuration.
    Returns:
        query result in the form of dataframe.
    """
    # convert a YAML file to a Python object
    config = get_yaml_config(configfile)
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
        df = pd.read_sql_query(sqlCommands, engine)
        pd.set_option(
            "display.expand_frame_repr", False
        )  # option: expand output display of dataframe
        return df
    except Exception as e:
        logging.error(
            "Error while returning Query results in Dataframe format", e
        )
    finally:
        engine.dispose()


def read_text_file(filename):
    """
    Read text file.

    Args:
        filename: the name of file you want to read.
    Returns:
        contents in the file.
    """
    logging.info("Reading file: " + filename)
    try:
        # Open and read SQL file
        with open(filename, "r") as fd:
            FileContents = fd.read()
        return FileContents
    except Exception:
        logging.error("Error while reading %s ...", filename)


def commit_dataframe(df, tablename, configfile, target_schema):
    """
    Load contents from a dataframe into a table in database.

    Args:
        df: dataframe name.
        tablename: Name of PostgreSQL table.
        configfile: database configuration.
        target_schema: table schema.
    """
    # convert a YAML file to a Python object
    config = get_yaml_config(configfile)
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
        logging.error(
            "Error while returning Query results in Dataframe format.", e
        )
    finally:
        engine.dispose()


def import_csv(sqlfilename, csv_path, configfile):
    """
    Run a sql file, import a csv file to postgres, generate a table.

    Args:
        sqlfilename: SQL scripts.
        csv_path: the path where you want to import the csv file
        (refer to mount path in docker compose).
        configfile: database configuration.
    """
    # convert a YAML file to a Python object
    config = get_yaml_config(configfile)
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
    s = read_text_file(sqlfilename)
    # Import csv file
    with open(csv_path) as file:
        cursor.copy_expert(s, file)

    try:
        logging.info(
            "Importing a csv file %s to postgres, generating a table...", csv_path
        )
        conn.commit()
    except Exception as e:
        logging.error("Error while importing a csv file to postgres.", e)


def export_to_csv(sqlfilename, csv_path, configfile):
    """
    Run a sql file, export result table to a .csv file.

    Args:
        sqlfilename: SQL scripts.
        csv_path: the path where you want to generate the csv file
        (refer to mount path in docker compose).
        configfile: database configuration.
    """
    # convert a YAML file to a Python object
    config = get_yaml_config(configfile)
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
    s = read_text_file(sqlfilename)
    # Convert to csv file
    with open(csv_path, "w") as file:
        cursor.copy_expert(s, file)
    try:
        logging.info("Exporting result table to a .csv file %s...", csv_path)
        conn.commit()
    except Exception as e:
        logging.error("Error while exporting result table to a .csv file.", e)


def dataframe_to_csv(dataframe, csv_path, table, configfile):
    """
    Load contents from a dataframe into a table in database, export to a csv file eventually.

    Args:
        dataframe: dataframe name.
        csv_path: the path where you want to generate the csv file
        (refer to mount path in docker compose).
        table: target table in the database.
        configfile: database configuration.
    """
    # convert a YAML file to a Python object
    config = get_yaml_config(configfile)
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=f'{config["hostname"]}',
        port=f'{config["portnumber"]}',
        database=f'{config["dbname"]}',
        user=f'{config["dbUser"]}',
        password=f'{config["dbPass"]}',
    )
    cursor = conn.cursor()

    dataframe.to_csv(csv_path, index=False)
    f = open(csv_path, "r")

    try:
        cursor.copy_from(f, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as err:
        os.remove(csv_path)
        # pass exception to function
        show_psycopg2_exception(err)
        cursor.close()


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


def sparql_query(endpoint, query):
    """
    Return Sparql query results (with headers) in DataFrame format.

    Args:
        endpointLocation: SPARQL Endpoint URL.
        (HTTP network which is capable of receiving and processing SPARQL Protocol requests).
        query: SPARQL query.
    """
    sparql = SPARQLWrapper(endpoint)

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


def RDFDB_clear(endpointLocation, rdfGraph=None):
    """
    Clear the repository if repository already existed.
    Args:
        endpointLocation: SPARQL Endpoint URL
        (HTTP network which is capable of receiving and processing SPARQL Protocol requests).
        rdfGraph (optional): Resource Description Framework name.
    """
    sparql = SPARQLWrapper(endpointLocation + "/statements")
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


def RDFDB_insert(endpointUpdate, triple_store_path, rdfGraph=None):
    """
    Send the mapped data to the repository.
    Args:
        endpointLocation: SPARQL Endpoint URL
        (HTTP network which is capable of receiving and processing SPARQL Protocol requests).
        triple_store_path: the path of RDF triplestore
        (a graph database that stores semantic facts).
        rdfGraph (optional): Resource Description Framework name.
    """
    # Read ttl file
    with open(triple_store_path, "r") as fd:
        ttlFile = fd.read()

    sparql = SPARQLWrapper(endpointUpdate + "/update")
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


def RDFDB_create(endpointLocation, rdfRepository):
    """
    Create remote repositories on rdf4j automatically.
    Args:
        endpointLocation: SPARQL Endpoint URL
        (HTTP network which is capable of receiving and processing SPARQL Protocol requests).
        rdfRepository: rdf repository ID.
    """
    temp = f"""
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
    @prefix rep: <http://www.openrdf.org/config/repository#>.
    @prefix sr: <http://www.openrdf.org/config/repository/sail#>.
    @prefix sail: <http://www.openrdf.org/config/sail#>.
    @prefix ms: <http://www.openrdf.org/config/sail/memory#>.
    [] a rep:Repository ;
       rep:repositoryID "{rdfRepository}" ;
       rdfs:label "{rdfRepository}" ;
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
        cURL_cmd = f"""curl -X PUT -H "Content-type: text/turtle" --data-binary @createrepo.ttl {endpointLocation}"""
        result = subprocess.Popen(cURL_cmd, shell=True, universal_newlines=True).wait()
        logging.info("Creating remote repositories on rdf4j...")
    except Exception as e:
        logging.error("Error while creating remote repositories on rdf4j.", e)


def sparqlQuery_return_XML(endpoint, query):
    """
    Return Sparql query results (with headers) in XML format.
    Args:
        endpointLocation: SPARQL Endpoint URL
        (HTTP network which is capable of receiving and processing SPARQL Protocol requests).
        query: SPARQL query.
    """
    sparql = SPARQLWrapper(endpoint)

    # read SPARQL query
    with open(query, "r") as fd:
        querytext = fd.read()
        sparql.setQuery(querytext)

    try:
        sparql.setReturnFormat(XML)
        results = sparql.query().convert()
        logging.info("Return SPARQL Query results in XML format successfully.")
    except Exception as e:
        logging.error("Error while returning SPARQL Query results in XML format.", e)

    return results.toxml()


def sparqlQuery_return_JSON(endpoint, query):
    """
    Return Sparql query results (with headers) in JSON format.
    Args:
        endpointLocation: SPARQL Endpoint URL
        (HTTP network which is capable of receiving and processing SPARQL Protocol requests).
        query: SPARQL query.
    """
    sparql = SPARQLWrapper(endpoint)

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
        logging.error("Error while returning SPARQL Query results in JSON format.", e)

    return result

