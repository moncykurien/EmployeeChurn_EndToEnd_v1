from datetime import datetime
import logging

class Logger:
    """
    **********************************************************************************
    *
    * file name : logger.py
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
    *   Description: Class to generate the logs
    *
    **********************************************************************************
    """

    def __init__(self, run_id, log_module, log_file_name):
        self.logger = logging.getLogger(str(log_module)+"_"+str(run_id))
        self.logger.setLevel(logging.DEBUG)

        if log_file_name == 'training':
            file_handler = logging.FileHandler('logs/training_logs/train_log_'+str(run_id)+'.log')
        else:
            file_handler = logging.FileHandler('logs/prediction_logs/predict_log_'+str(run_id)+'.log')

        formatter = logging.Formatter('%(asctime)s  :   %(levelname)s   :   %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)


    def info(self, message):
        """
        **********************************************************************************
        *
        * method : info
        * parameters : message - Type: String
        * description : Method to log information in log file
        * return : none
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
        self.logger.info(message)

    def exception(self, message):
        """
        **********************************************************************************
        *
        * method : exception
        * parameters : message - Type: String
        * description : Method to log exceptions in log files
        * return : none
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
        self.logger.exception(message)