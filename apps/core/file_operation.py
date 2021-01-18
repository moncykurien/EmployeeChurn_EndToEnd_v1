import pickle
from apps.core.logger import Logger
import os
import shutil


class FileOperation:
    """
    **********************************************************************************
    *
    * file name : file_operation.py
    * version : 1.0
    * author : Moncy Kurien
    * creation date : 04-Jan-2021
    *
    *
    * change history:
    *
    *   who           when            version     change(include bug # if apply)
    *   ----------    -------         --------    -----------------------------
    *   Moncy Kurien  04-Jan-2021     1.0         Initial Creation
    *
    *   Description: Class for File operations
    *
    **********************************************************************************
    """

    def __init__(self, run_id, data_path, mode):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'FileOperation', mode)

    def save_model(self, model, file_name):
        """
                **********************************************************************************
                *
                * method : save_model
                * parameters : model: - Type(Object) : model object reference
                *               file_name : Type(String) : Name of the file
                * description : Method to save the ML model file
                * return : none - File gets saved
                *
                * change history:
                *
                *   who           when            version     change(include bug # if apply)
                *   ----------    -------         --------    -----------------------------
                *   Moncy Kurien  04-Jan-2021     1.0         Initial Creation
                *
                *
                *
                **********************************************************************************
        """
        try:
            self.logger.info('Start of Save Models')
            path = os.path.join('apps/models', file_name)  #create a separate directory for each cluster

            if os.path.isdir(path):
                shutil.rmtree('apps/models')
                os.makedirs(path)
            else:
                os.makedirs(path)

            with open(path+'/'+file_name+'.sav', 'wb') as f:
                pickle.dump(model, f)   #Save the model to the file

            self.logger.info('Model File '+file_name+' saved')
            self.logger.info('End of Save Models')
            return 'success'
        except Exception as e:
            self.logger.exception("Exception raised while Save Models: %s" %e)
            raise Exception()


    def load_model(self, file_name):
        """
                **********************************************************************************
                *
                * method : load_model
                * parameters :  file_name : Type(String) : Name of the file
                * description : Method to load the ML model from file file
                * return : returns de-serialized model object
                *
                * change history:
                *
                *   who           when            version     change(include bug # if apply)
                *   ----------    -------         --------    -----------------------------
                *   Moncy Kurien  04-Jan-2021     1.0         Initial Creation
                *
                *
                *
                **********************************************************************************
        """
        try:
            self.logger.info('Start of Load Model')
            with open('apps/models/'+file_name+'/'+file_name+'.sav', 'rb') as f:
                model = pickle.load(f)
                self.logger.info('Model File '+file_name+' loaded.')
                self.logger.info('End of Load Models')
                return model
        except Exception as e:
            self.logger.exception("Exception raised while Load Models: %s " %e)
            raise Exception()

    def correct_model(self, cluster_number):
        """
                **********************************************************************************
                *
                * method : correct_model
                * parameters :  cluster_number
                * description : Method to find the best model
                * return : the model file
                *
                * change history:
                *
                *   who           when            version     change(include bug # if apply)
                *   ----------    -------         --------    -----------------------------
                *   Moncy Kurien  04-Jan-2021     1.0         Initial Creation
                *
                *
                *
                **********************************************************************************
        """
        try:
            self.logger.info("Start of finding Correct Model.")
            self.cluster_number = cluster_number
            self.folder_name = 'apps/models'
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)

            for self.file in self.list_of_files:
                try:
                    #String.index(str(i)) will look at the string and returns the index/position of the str(i) in the String.
                    #If the str(i) is not found it errors.
                    if (self.file.index(str(self.cluster_number))!=-1):
                        self.model_name = self.file
                except:
                    continue
            self.model_name = self.model_name.split('.')[0]
            self.logger.info('End of Correct Model.')
            return self.model_name
        except Exception as e:
            self.logger.exception('Exception raised while finding Correct Model: %s ' %e)
            raise Exception()