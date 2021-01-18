from apps.core.logger import Logger
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from sklearn.model_selection import train_test_split
from apps.core.file_operation import FileOperation
from apps.preprocessing.preprocessor import Preprocessor
from apps.ingestion.load_validate import LoadValidate


class KMeanCluster:
    """
    *****************************************************************************
    *
    * filename:       cluster.py
    * version:        1.0
    * author:         Moncy Kurien
    * creation date:  05-MAY-2020
    *
    * change history:
    *
    * who             when           version  change (include bug# if apply)
    * ----------      -----------    -------  ------------------------------
    * moncykurien       05-MAY-2020    1.0      initial creation
    *
    *
    * description:    Class to cluster the dataset
    *
    ****************************************************************************
    """
    def __init__(self, run_id, data_path):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'KMeansCluster', 'training')
        self.fileOperation = FileOperation(self.run_id, self.data_path, 'training')

    def elbow_plot(self, data):
        """
        * method: log
        * description: method to saves the plot to decide the optimum number of clusters to the file.
        * return: A picture saved to the directory
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * moncykurien       05-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   data:
        """
        try:
            #an empty list to store the within cluster sum of errors
            wcss = []
            self.logger.info("Starting to cluster the data with elbow plot")
            for i in range(1, 11):
                #initializing kmeans object
                kmeans = KMeans(n_clusters=i, init='k-means++', n_jobs=-1)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)
            #plotting the elbow - no of clusters vs wcss
            plt.plot(range(1,11), wcss)
            plt.title("Elbow plot - Elbow method")
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            #saving the plot
            plt.savefig('apps/models/kmeans_elbow.png')
            #finding the optimum number of clusters programmatically
            self.kn = KneeLocator(range(1,11), wcss, curve = 'convex', direction = 'decreasing')
            self.logger.info('The optimum number of clusters is: '+str(self.kn.knee))
            self.logger.info('End of elbow plotting...')
            return self.kn.knee
        except Exception as e:
            self.logger.exception('Exception raised while elbow plotting:' + str(e))
            raise Exception()


    def create_clusters(self, data, number_of_clusters):
        self.data=data
        try:
            self.logger.info('Starting to create clusters with given number of clusters')
            self.kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', random_state=0)
            #predict the cluster number
            self.y_kmeans = self.kmeans.fit_predict(self.data)
            self.logger.info("Successfully created the clusters")
            save_model = self.fileOperation.save_model(self.kmeans,'kmeans')
            if save_model:
                self.logger.info("Cluster model successfully saved")
            self.data['cluster'] = self.y_kmeans
            self.logger.info("End of create clusters.")
            return self.data
        except Exception as e:
            self.logger.exception("Error occurred while creating clusters. %s" %e)
            raise Exception()



