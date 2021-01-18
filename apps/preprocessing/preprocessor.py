from apps.core.logger import Logger
import pandas as pd
from sklearn.impute import KNNImputer
import json


class Preprocessor:
    def __init__(self, run_id, datapath, mode):
        self.run_id = run_id
        self.data_path = datapath
        self.logger = Logger(self.run_id, 'Preprocessor', mode)

    def get_data(self):
        try:
            self.logger.info("Starting to get data from CSV to DataFrame.")
            self.data = pd.read_csv(self.data_path+'_validation/InputFile.csv')
            self.logger.info("End of reading data.")
            return self.data
        except Exception as e:
            self.logger.exception("Error occurred while reading data from Training set. %s" %e)
            raise Exception()

    def drop_columns(self, data, columns):
        self.data = data
        self.columns = columns
        try:
            self.logger.info("Starting drop columns")
            self.useful_data = self.data.drop(labels = self.columns, axis = 1)
            self.logger.info("End of dropping columns")
            return self.useful_data
        except Exception as e:
            self.logger.exception("Error occurred while dropping columns. %s" %e)
            raise Exception()

    def is_null_present(self, data):
        self.null_present = False
        try:
            self.logger.info("Starting to find missing values")
            self.null_counts = data.isna().sum()
            for i in self.null_counts:
                if i > 0:
                    self.null_present = True
                    break
            if (self.null_present):
                dataframe_with_null = pd.DataFrame(data.isna().sum())
                dataframe_with_null.reset_index(inplace=True)
                dataframe_with_null.rename(columns={'index':'columns', 0:'Missing values counts'}, inplace=True)
                dataframe_with_null.to_csv(self.data_path+'_validation/null_values.csv')
            self.logger.info("End of finding missing values.")
            return self.null_present
        except Exception as e:
            self.logger.exception("Error occurred while finding missing values. %s" %e)
            raise Exception()


    def impute_missing_values(self, data):
        self.data = data
        try:
            self.logger.info('Starting to impute the missing values.')
            imputer = KNNImputer(n_neighbors=3)
            #treat the missing values
            self.new_array = imputer.fit_transform(self.data)
            #convert the np array from above step to DataFrame
            self.new_data = pd.DataFrame(data=self.new_array, columns=self.data.columns)
            self.logger.info('End of impute missing values.')
            return self.new_data
        except Exception as e:
            self.logger.exception("Error occurred while imputing the missing values. %s" %str(e))
            raise Exception()

    def feature_encoding(self, data):
        try:
            self.logger.info('Starting of feature encoding')
            self.new_data = data.select_dtypes(include=['object']).copy()
            #Encoding the categorical columns to numeric using get dummies
            self.new_data = pd.get_dummies(self.new_data, prefix=self.new_data.columns.to_list(), drop_first=True)
            self.logger.info("End of feature encoding")
            return self.new_data
        except Exception as e:
            self.logger.exception("Error occurred while feature encoding. %s" %e)
            raise Exception()


    def split_features_label(self, data, label_name):
        self.data = data
        try:
            self.logger.info("Starting to split features and labels")
            self.X = self.data.drop(labels=label_name, axis=1)
            self.y = self.data[label_name]
            self.logger.info("End of spliting features and labels")
            return self.X,self.y
        except Exception as e:
            self.logger.exception("Error occurred while splitting data into Features and labels. %s" %e)
            raise Exception()


    def preprocess_predictset(self):
        try:
            self.logger.info('Start of Preprocessing predictset.')
            data = self.get_data()
            #Not dropping the Empid here since we need it to save the predictions in a file
            #data = self.drop_columns(data, ['empid'])
            cat_df = self.feature_encoding(data)
            data = pd.concat([data,cat_df], axis = 1)
            data = self.drop_columns(data, ['salary'])
            is_null_present = self.is_null_present(data)
            if (is_null_present):
                data = self.impute_missing_values(data)
                
            data = self.final_predictset(data)
            self.logger.info("End of Preprocessing Predictset.")
            return data
        except Exception as e:
            self.logger.exception('Error occurred while Preprocessing Predictset. %s' %e)
            raise Exception()

    def final_predictset(self,data):
        try:
            self.logger.info("start of final predictset.")
            with open('apps/database/columns.json', 'r') as f:
                data_columns = json.load(f)['data_columns']
            #create an empty dataframe with the columns
            df = pd.DataFrame(data=None, columns=data_columns)
            #concat the new dataframe with the predictset data. If any column is missing in the predictset, it will be added with NaNs
            new_df = pd.concat([df,data], ignore_index=True, sort=False)
            #fill the NaN values with 0
            data_new = new_df.fillna(0)
            self.logger.info("End of building the final predictset.")
            return data_new
        except ValueError:
            self.logger.exception('ValueError occurred while building final predictset.')
            raise ValueError
        except KeyError:
            self.logger.exception('KeyError occurred while building final predictset.')
            raise KeyError
        except Exception as e:
            self.logger.exception("Error occurred while building final predictset. %s" %e)
            raise Exception()



    def preprocess_trainset(self):
        try:
            self.logger.info("Start to Preprocess.")
            #Get data into a DataFrame from csv file
            data = self.get_data()
            #drop the unwanted columns
            data = self.drop_columns(data, ['empid'])
            #handle categorical features
            cat_df = self.feature_encoding(data)
            data = pd.concat([data, cat_df], axis = 1)
            #drop the categorical columns
            data = self.drop_columns(data, ['salary'])
            #check if missing values are present
            is_null_present = self.is_null_present(data)
            #impute missing values if present
            if (is_null_present):
                data = self.impute_missing_values(data)
            #Split data in X and y
            self.X, self.y = self.split_features_label(data, label_name='left')
            self.logger.info('End of Preprocessing Trainset.')
            return self.X, self.y
        except Exception as e:
            self.logger.exception("Error occurred while preprocessing the training set. %s" %e)
            raise Exception()


    def preprocess_predict(self, data):
        try:
            self.logger.info("Start of preprocessing predict.")
            cat_df = self.feature_encoding(data)
            data = pd.concat([data, cat_df], axis=1)
            data = self.drop_columns(data, ['salary'])
            is_null_present = self.is_null_present(data)
            if (is_null_present):
                data = self.impute_missing_values(data)

            data = self.final_predictset(data)
            self.logger.info("End of Preprocessing Predictset.")
            return data
        except Exception as e:
            self.logger.exception('Error occurred while Preprocessing Predictset. %s' % e)
            raise Exception()

