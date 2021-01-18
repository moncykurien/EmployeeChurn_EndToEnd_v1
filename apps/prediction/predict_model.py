from apps.core.logger import Logger
from apps.ingestion.load_validate import LoadValidate
from apps.preprocessing.preprocessor import Preprocessor
from apps.core.file_operation import FileOperation
import pandas as pd
import csv

class PredictModel:
    def __init__(self, run_id, data_path):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'PredictModel', self.data_path)
        self.loadValidate = LoadValidate(self.run_id, self.data_path, 'prediction')
        self.preProcess = Preprocessor(self.run_id, self.data_path, 'prediction')
        self.fileOperation = FileOperation(self.run_id,self.data_path, 'prediction')

    def batch_predict_from_model(self):
        try:
            self.logger.info("Start of batch Predict from model.")
            self.logger.info(f"Run id is {self.run_id}")
            #Load,Validate and Transform the prediction dataset
            self.loadValidate.validate_predictset()
            #Preprocess the Predictset
            self.X = self.preProcess.preprocess_predictset()
            #load the KMeans cluster algorithm
            kmeans = self.fileOperation.load_model('kmeans')
            #predict the clusters for the predictset for X
            clusters = kmeans.predict(self.X.drop(['empid'], axis = 1))
            #add the predicted clusters to the data
            self.X['cluster'] = clusters
            #get the unique clusters
            clusters = self.X['cluster'].unique()
            #initialise list for predictions
            y_predictions = []
            final_predictions = pd.DataFrame(data = None, columns=['EmpId', 'Predictions'])
            #predict data based on the clusters
            for i in clusters:
                self.logger.info("fStarting to predict data within cluster {i}")
                #extract the date based on the cluster
                cluster_data = self.X[self.X['cluster'] == i]
                #Remove unwanted columns
                cluster_data_new = cluster_data.drop(['empid','cluster'], axis=1)
                #Find the model for each cluster
                model_name = self.fileOperation.correct_model(i)
                #load the model
                model = self.fileOperation.load_model(model_name)
                #predict the label
                y_predictions = model.predict(cluster_data_new)
                self.logger.info(f"Prediction of label for cluster {i} successful.")

                result = pd.DataFrame({'EmpId': cluster_data['empid'].astype('int64'), 'Predictions': y_predictions})
                final_predictions = pd.concat([final_predictions, result], ignore_index=True)
                self.logger.info(f"Cluster {i} Predictions appended to final dataframe.")

            final_predictions.to_csv(self.data_path+'_results/'+'Predictions.csv', header=True, index=False, sep=',', line_terminator='\r\n',quoting=csv.QUOTE_ALL,escapechar='\\')
            self.logger.info(f"Cluster {i} Predictions stored in {self.data_path}_results/Predictions.csv. ")
            self.logger.info('End of Predictions.')
        except Exception as e:
            self.logger.exception("Error occurred while batch predicting from model.")
            raise Exception


    def single_predict_from_model(self, data):
        try:
            self.logger.info("Start of Single Predict from model.")
            self.logger.info(f"Run id is {self.run_id}")
            #Preprocess the Predictset
            self.X = self.preProcess.preprocess_predict(data)
            #load the KMeans cluster algorithm
            kmeans = self.fileOperation.load_model('kmeans')
            #predict the clusters for the predictset for X
            clusters = kmeans.predict(self.X.drop(['empid'], axis = 1))
            #add the predicted clusters to the data
            self.X['cluster'] = clusters
            #get the unique clusters
            clusters = self.X['cluster'].unique()
            #initialise list for predictions
            y_predictions = []

            #predict data based on the clusters
            for i in clusters:
                self.logger.info("fStarting to predict data within cluster {i}")
                #extract the date based on the cluster
                cluster_data = self.X[self.X['cluster'] == i]
                #Remove unwanted columns
                cluster_data_new = cluster_data.drop(['empid','cluster'], axis=1)
                #Find the model for each cluster
                model_name = self.fileOperation.correct_model(i)
                #load the model
                model = self.fileOperation.load_model(model_name)
                #predict the label
                y_predictions = model.predict(cluster_data_new)
                self.logger.info(f"Prediction of label for cluster {i} successful. Prediction is: {y_predictions}")

            self.logger.info('End of Single Predictions.')
            return int(y_predictions[0])
        except Exception as e:
            self.logger.exception("Error occurred while doing Single prediction from model.")
            raise Exception