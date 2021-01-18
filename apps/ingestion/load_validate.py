import json
from os import listdir
import shutil
import pandas as pd
from datetime import datetime
import os
from apps.database.database_operation import DatabaseOperation
from apps.core.logger import Logger

class LoadValidate:
    """
        *****************************************************************************
        *
        * filename:       LoadValidate.py
        * version:        1.0
        * author:         Moncy Kurien
        * creation date:  05-MAY-2020
        *
        * change history:
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * Moncy Kurien      05-MAY-2020    1.0      initial creation
        *
        *
        * description:    Class to load, validate and transform the data
        *
        ****************************************************************************
        """
    def __init__(self, run_id, data_path, mode):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id,'ValidateLoad', mode)
        self.dbOperation = DatabaseOperation(self.run_id, self.data_path, mode)

    def values_from_schema(self, schema_file):
        """
                * method: values_from_schema
                * description: method to read schema file
                * return: column_names, Number of Columns
                *
                * who             when           version  change (include bug# if apply)
                * ----------      -----------    -------  ------------------------------
                * moncykurien       05-MAY-2020    1.0      initial creation
                *
                * Parameters
                *   schema_file:
                """
        try:
            self.logger.info("Starting of reading values from schema")
            with open('apps/database/'+schema_file+'.json', 'r') as f:
                dic = json.load(f)

            column_names = dic['ColName']
            number_of_columns = dic['NumberOfColumns']
            self.logger.info("End of reading values from schema.")
            return column_names, number_of_columns

        except ValueError:
            self.logger.exception("ValueError occurred while reading values from schema")
            raise ValueError

        except KeyError:
            self.logger.exception('KeyError occurred while reading values from schema')
            raise KeyError

        except Exception as e:
            self.logger.exception("Error occurred while reading values from schema: %s" %e)
            raise e


    def validate_column_length(self, number_of_columns):
        """
        * method: validate_column_length
        * description: method to validates the number of columns in the csv files
        * return: none
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * moncykurien       05-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   number_of_columns:
        """
        try:
            self.logger.info("Starting to validate the column length.")
            for file in listdir(self.data_path):
                csv = pd.read_csv(self.data_path+'/'+file)
                if csv.shape[1] == number_of_columns:
                    pass
                else:
                    shutil.move(self.data_path+'/'+file, self.data_path+'_rejects')
                    self.logger.info(f'Invalid column length in file {file}. Expected {number_of_columns}, but found {csv.shape[1]}')
            self.logger.info('End of validating the column length.')
        except OSError:
            self.logger.exception('OSError raised while validating the lenght of column.')
            raise OSError
        except Exception as e:
            self.logger.exception('Error occurred while validating the length of columns. %s' %e)
            raise e


    def validating_missing_values(self):
        """
        * method: validate_missing_values
        * description: method to validates if any column in the csv file has all values missing.
        *              If all the values are missing, the file is not suitable for processing. it to be moved to bad file
        * return: none
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * moncykurien       05-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   none:
        """
        try:
            self.logger.info("Starting to validate the missing values")
            for file in listdir(self.data_path):
                csv = pd.read_csv(self.data_path+'/'+file)
                for column in csv:
                    if csv[column].count() == 0:
                        shutil.move(self.data_path+'/'+file, self.data_path+'_rejects')
                        self.logger.info("All the values in Column {column} are missing in file {file}. Moved to rejects folder.".format(column=column,file=file))
                        break

            self.logger.info("Ending missing values validation.")
        except OSError:
            self.logger.exception(f"OSError occurred while validating missing values in {file}")
            raise OSError
        except Exception as e:
            self.logger.exception(f"Error occurred while validating missing values in {file}. {e}")


    def replace_missing_values(self):
        """
        * method: replace_missing_values
        * description: method to replaces the missing values in columns with "NULL"
        * return: none
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * moncykurien       05-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   none:
        """
        try:
            self.logger.info("Starting to replace missing values with Null")
            for file in listdir(self.data_path):
                csv = pd.read_csv(self.data_path+'/'+file)
                csv.fillna('NULL', inplace=True)
                csv.to_csv(self.data_path+'/'+file, index=None, header=True)
            self.logger.info("End of replacing missing values.")
        except Exception as e:
            self.logger.exception("Error occurred while replacing missing values. %s" %e)
            raise e


    def archive_old_files(self):
        """
        * method: archive_old_rejects
        * description: method to archive rejected files
        * return: none
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * moncykurien       05-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   none:
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime('%H.%M.%S')

        def archive_helper(path, source, dest_name, date, time):
            """
            A small sub class to help the archiving process.
            :param self:
            :param path:
            :param source:
            :param dest_name:
            :param date:
            :param time:
            :return: none
            """
            if os.path.isdir(source):
                dest = path + dest_name + str(date) + '_' + str(time)
                files = os.listdir(source)
                for file in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if file not in os.listdir(dest):
                        shutil.move(source+'/'+file, dest)

        try:

            path = self.data_path+'_archive'
            if not os.path.isdir(path):
                os.makedirs(path)

            self.logger.info("Starting to Archive Old Rejected files.")
            source = self.data_path+'_rejects'
            archive_helper(path=path, source=source, dest_name='/reject_', date=date, time=time)
            self.logger.info("End of Archiving Old Rejected files.")

            self.logger.info("Starting to Archive Old Validated Files")
            source = self.data_path+'_validation'
            archive_helper(path, source, '/validation_', date, time)
            self.logger.info("End of Archiving Old Validated files.")

            self.logger.info("Starting to Archive Old Processed files.")
            source = self.data_path+'_processed'
            archive_helper(path, source, '/preprocessed_', date, time)
            self.logger.info("End of Archiving Old Processed files.")

            self.logger.info("Starting to Archive Old Results Files")
            source = self.data_path+'_results'
            archive_helper(path, source, '/results_', date, time)
            self.logger.info("End of Archiving Old Results files.")

            self.logger.info("End of Archiving process")

        except Exception as e:
            self.logger.exception('Error occurred while Archiving the old files. %s' %e)
            raise e


    def move_processed_file(self):
        """
        * method: move_processed_files
        * description: method to move processed files
        * return: none
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * moncykurien       05-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   none:
        """
        try:
            self.logger.info("Starting to move the processed files.")
            files = os.listdir(self.data_path)
            for file in files:
                shutil.move(self.data_path+'/'+file, self.data_path+'_processed')
                self.logger.info(f"Moved the processed file {file}.")
            self.logger.info("Ending the moving of Processed files.")

        except Exception as e:
            self.logger.exception("Error occurred while moveing the processed files. %s" %e)
            raise e

    def is_file_available(self):
        try:
            if len(os.listdir(self.data_path)) > 0:
                return True
            else:
                return False
        except Exception as e:
            self.logger.exception("Error occurred while checking if new file is available. %s" %e)
            raise Exception()

    def validate_trainset(self):
        """
        * method: validate_training
        * description: method to validate the data
        * return: none
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * moncykurien       05-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   none:
        """
        try:
            self.logger.info("Start of Training Data Load, Validation and transformation.")
            #Archive the old files
            if self.is_file_available():
                self.archive_old_files()
                #Get the column names and number of columns from training schema
                column_names, number_of_columns = self.values_from_schema('schema_train')
                #Validate the column length
                self.validate_column_length(number_of_columns)
                #Validate if any column is fully empty
                self.validating_missing_values()
                #Replace the missing values(blanks) with Null
                self.replace_missing_values()
                #Create a database with the given name, establish connection is exists. Create table with the given colume names
                self.dbOperation.create_table('training','training_raw_data_t',column_names)
                #insert csv file into the table
                self.dbOperation.insert_data('training','training_raw_data_t')
                #Export data from table to a csv file
                self.dbOperation.export_csv('training','training_raw_data_t')
                #Move the processed file
                self.move_processed_file()
                self.logger.info("End of Training Data Load, Validation and transformation.")
            else:
                self.logger.info(f"No file in {self.data_path} to Load, Validation and transformation.")

        except Exception as e:
            self.logger.exception("Error occurred while Training Data Load, Validation and transformation. %s" %e)
            raise e

    def validate_predictset(self):
        """
        * method: validate
        * description: method to validate the predict data
        * return: none
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * moncykurien       05-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   none:
        """
        try:
            self.logger.info('Start of Data Load, validation and transformation')
            if self.is_file_available():
                # archive old rejected files
                self.archive_old_files()
                # extracting values from schema
                column_names, number_of_columns = self.values_from_schema('schema_predict')
                # validating column length in the file
                self.validate_column_length(number_of_columns)
                # validating if any column has all values missing
                self.validating_missing_values()
                # replacing blanks in the csv file with "Null" values
                self.replace_missing_values()
                # create database with given name, if present open the connection! Create table with columns given in schema
                self.dbOperation.create_table('prediction', 'prediction_raw_data_t', column_names)
                # insert csv files in the table
                self.dbOperation.insert_data('prediction', 'prediction_raw_data_t')
                # export data in table to csv file
                self.dbOperation.export_csv('prediction', 'prediction_raw_data_t')
                # move processed files
                self.move_processed_file()
                self.logger.info('End of Data Load, validation and transformation')
            else:
                self.logger.info(f"No file in {self.data_path} to Load, Validation and transformation.")
        except Exception:
            self.logger.exception('Unsuccessful End of Data Load, validation and transformation')
            raise Exception