import sqlite3
import csv
from os import listdir
import shutil
import os
import os.path

from apps.core.logger import Logger

class DatabaseOperation:
    """
    **********************************************************************************
    *
    * file name : database_operation.py
    * version : 1.0
    * author : Moncy Kurien
    * creation date : 05-Jan-2021
    *
    *
    * change history:
    *
    *   who           when            version     change(include bug # if apply)
    *   ----------    -------         --------    -----------------------------
    *   Moncy Kurien  05-Jan-2021     1.0         Initial Creation
    *
    *   Description: Class to handle database operations
    *
    **********************************************************************************
    """

    def __init__(self, run_id, data_path, mode):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'DatabaseOperation', mode)


    def database_connection(self, database_name):
        """
        **********************************************************************************
        *
        * method : database_connection
        * parameters : database_name: Type - String. Name of the database to connect.
        * description : Method to build a database connection
        * return : sqlite3.connect object. Connection to the database
        *
        * change history:
        *
        *   who           when            version     change(include bug # if apply)
        *   ----------    -------         --------    -----------------------------
        *   Moncy Kurien  05-Jan-2021     1.0         Initial Creation
        *
        *
        *
        **********************************************************************************
        """
        try:
            con = sqlite3.connect('apps/database/'+database_name+'.db')
            self.logger.info('Connected to the %s database successfully.' %database_name)
        except ConnectionError as e:
            self.logger.exception('Error while connecting to database: %s. ' %e)
            raise ConnectionError
        return con

    def create_table(self, database_name, table_name, column_names):
        """
        **********************************************************************************
        *
        * method : create_table
        * parameters : database_name: Name of the database to connect.
        *               table_name: Name of the table
        *               column_names: Type-Dictionary, Name of the columns as key and the data type as value
        * description : Method to create a table
        * return : none. Creates the table
        *
        * change history:
        *
        *   who           when            version     change(include bug # if apply)
        *   ----------    -------         --------    -----------------------------
        *   Moncy Kurien  05-Jan-2021     1.0         Initial Creation
        *
        *
        *
        **********************************************************************************
        """
        try:
            self.logger.info(f"Start of Creating Table {table_name} in database {database_name}.")
            conn = self.database_connection(database_name)

            if database_name == 'prediction':
                conn.execute("DROP TABLE IF EXISTS '"+table_name+"';")

            c = conn.cursor()
            c.execute("SELECT COUNT(NAME) FROM SQLITE_MASTER WHERE TYPE = 'TABLE' AND NAME = '"+table_name+"'")

            if c.fetchone()[0] == 1:
                for key in column_names.keys():
                    type = column_names[key]

                    try:
                        conn.execute("ALTER TABLE "+table_name+" ADD COLUMN {column_name} {data_type}".format(column_name=key, data_type = type))
                        self.logger.info("Altered the existing table %s" %table_name)
                    except:
                        conn.execute("CREATE TABLE "+table_name+" ({column_name} {data_type})".format(column_name=key,data_type=type))
                        self.logger.info("Created the table %s" %table_name)
            else:
                for key in column_names.keys():
                    type = column_names[key]

                    try:
                        conn.execute("ALTER TABLE " + table_name + " ADD COLUMN {column_name} {data_type}".format(column_name=key, data_type=type))
                        self.logger.info("Altered the table %s" % table_name)
                    except:
                        conn.execute("CREATE TABLE " + table_name + " ({column_name} {data_type})".format(column_name=key,data_type=type))
                        self.logger.info("Created the table %s" % table_name)

            conn.commit()
            conn.close()
            self.logger.info('End of creating table.')
        except Exception as e:
            self.logger.info('Current working directory: '+os.getcwd())
            self.logger.exception("Error occurred while creating table: %s" %e)
            raise e


    def insert_data(self, database_name, table_name):
        """
        **********************************************************************************
        *
        * method : insert_data
        * parameters : database_name: Name of the database to connect.
        *               table_name: Name of the table
        *
        * description : Method to insert data into the table
        * return : none. Inserts data into the table
        *
        * change history:
        *
        *   who           when            version     change(include bug # if apply)
        *   ----------    -------         --------    -----------------------------
        *   Moncy Kurien  05-Jan-2021     1.0         Initial Creation
        *
        *
        *
        **********************************************************************************
        """
        conn = self.database_connection(database_name)
        good_data_path = self.data_path
        bad_data_path = self.data_path+"_rejects"

        only_files = [f for f in listdir(good_data_path)]
        self.logger.info("Starting to Insert data into table %s" %table_name)

        for file in only_files:
            try:
                with open(good_data_path+"/"+file, 'r') as f:
                    next(f)  #Skipping the header
                    reader = csv.reader(f, delimiter = ',')
                    for line in enumerate(reader):
                        to_db = ''
                        for value in (line[1]):
                            try:
                                to_db += "'"+value+"',"
                            except Exception as e:
                                raise e
                        to_db = to_db.rstrip(',')
                        conn.execute("INSERT INTO "+table_name+" VALUES ({values})".format(values=(to_db)))
                        conn.commit()

            except Exception as e:
                conn.rollback()
                shutil.move(good_data_path+"/"+file, bad_data_path)
                self.logger.exception("Error occurred while inserting rows into the table %s" %e)
                conn.close()
        conn.close()
        self.logger.info("End of inserting data into table.")


    def export_csv(self, database_name, table_name):
        """
        **********************************************************************************
        *
        * method : export_csv
        * parameters : database_name: Name of the database to connect.
        *               table_name: Name of the table
        *
        * description : Method to export data from table to a csv file
        * return : none. Export data into a csv file
        *
        * change history:
        *
        *   who           when            version     change(include bug # if apply)
        *   ----------    -------         --------    -----------------------------
        *   Moncy Kurien  05-Jan-2021     1.0         Initial Creation
        *
        *
        *
        **********************************************************************************
        """
        self.file_from_db = self.data_path+str('_validation/')
        self.file_name = 'InputFile.csv'
        try:
            self.logger.info('Start of Exporting Data into CSV...')
            conn = self.database_connection(database_name)
            sqlSelect = "SELECT *  FROM "+table_name+""
            cursor = conn.cursor()
            cursor.execute(sqlSelect)
            results = cursor.fetchall()
            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]
            #Make the CSV ouput directory
            if not os.path.isdir(self.file_from_db):
                os.makedirs(self.file_from_db)
            # Open CSV file for writing.
            csv_file = csv.writer(open(self.file_from_db + self.file_name, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')
            # Add the headers and data to the CSV file.
            csv_file.writerow(headers)
            csv_file.writerows(results)
            self.logger.info('End of Exporting Data into CSV...')
        except Exception as e:
            self.logger.exception('Exception raised while Exporting Data into CSV: %s ' %e)


