from datetime import datetime
import random

class Config:
    """
    **********************************************************************************
    *
    * file name : config.py
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
    *   Description: Class for configuration instance attributes
    *
    **********************************************************************************
    """

    def __init__(self):
        self.training_data_path = 'data/training_data'
        self.training_database = 'training'
        self.prediction_data_path ='data/prediction_data'
        self.prediction_database = 'prediction'

    def get_run_id(self):
        """
        **********************************************************************************
        *
        * method : get_run_id
        * parameters : none
        * description : Method to generate a run id
        * return : run id in String type
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

        self.now = datetime.now()
        self.data = self.now.date()
        self.current_time = self.now.strftime("%H.%M.%S")
        return str(self.data)+"_"+str(self.current_time)+"_"+str(random.randint(100000000, 999999999))