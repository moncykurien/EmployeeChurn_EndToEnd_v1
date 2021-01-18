from apps.core.logger import Logger
from apps.ingestion.load_validate import LoadValidate
from apps.core.file_operation import FileOperation
from apps.tuning.model_tuner import ModelTuner
from apps.preprocessing.preprocessor import Preprocessor
import json
from apps.tuning.cluster import KMeanCluster
from sklearn.model_selection import train_test_split


class TrainModel:
    def __init__(self, run_id, datapath):
        self.run_id = run_id
        self.data_path = datapath
        self.logger = Logger(self.run_id,'TrainModel', 'training')
        self.loadValidate = LoadValidate(self.run_id, self.data_path, 'training')
        self.fileOperation = FileOperation(self.run_id, self.data_path, 'training')
        self.preProcess = Preprocessor(self.run_id, self.data_path, 'training')
        self.cluster = KMeanCluster(self.run_id, self.data_path)
        self.modelTuner = ModelTuner(self.run_id, self.data_path, 'training')


    def training_model(self):
        try:
            self.logger.info("Starting to train the model.")
            self.logger.info("Run Id: "+str(self.run_id))
            #Load, validate and transform the raw Training file
            #uncomment the below line if new training data files need to be loaded.
            self.loadValidate.validate_trainset()
            #preprocess the training set and get dependent and independent variables
            self.X, self.y = self.preProcess.preprocess_trainset()
            #Store the post preprocessing columns available
            columns = {"data_columns":[col for col in self.X.columns]}
            with open('apps/database/columns.json', 'w') as f:
                f.write(json.dumps(columns))
            #get number of optimal clusters in the data
            number_of_clusters = self.cluster.elbow_plot(self.X)
            #Create clusters with the number_of_clusters found above
            self.X = self.cluster.create_clusters(self.X, number_of_clusters)
            #Add y to X so that we can Group the data with respect to the clusters
            self.X['labels'] = self.y
            #Get the unique cluster values
            list_of_clusters = self.X['cluster'].unique()
            #Grouping as per the clusters
            for i in list_of_clusters:
                self.logger.info(f"Starting to get best model for cluster {i}")
                cluster_data = self.X[self.X['cluster'] == i]
                #split data into features and labels
                cluster_features = cluster_data.drop(['cluster','labels'], axis = 1)
                cluster_labels = cluster_data['labels']
                #split into train and test set
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_labels, stratify=cluster_labels, test_size = 0.2, random_state=0)
                #getting the best model for each cluster
                best_model_name, best_model = self.modelTuner.get_best_model(x_train, y_train, x_test, y_test)

                #saving the best model
                save_model = self.fileOperation.save_model(best_model, best_model_name+str(i))
                self.logger.info(f"Saved {best_model_name} as the best model for cluster {i}.")

            self.logger.info("End of Training model.")
        except Exception as e:
            self.logger.exception("Error occurred while training Model in train_model.py. %s" %e)
            raise Exception()