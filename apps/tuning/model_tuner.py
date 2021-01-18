from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, accuracy_score, r2_score
from apps.core.logger import Logger

class ModelTuner:
    """
    *****************************************************************************
    *
    * filename:       model_tuner.py
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
    * description:    Class to tune and select best model
    *
    ***********
    """
    def __init__(self, run_id, data_path, mode):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'ModelTuner', mode)
        self.rfc = RandomForestClassifier()
        self.xgb = XGBClassifier(objective='binary:logistic')


    def best_params_randomforest(self, train_x, train_y):
        try:
            self.logger.info("Start of finding the best parameters for RandomForestClassifier.")
            #Parameter grid
            self.param_grid = {
                'n_estimators' :[50, 100, 120, 150, 180, 200],
                'criterion' : ['gini', 'entropy'],
                'max_depth' : list(range(2,11)),
                'max_features' : ['auto','log2',None]
                }
            #instantiating GridSearchCV
            self.grid = GridSearchCV(estimator=self.rfc, param_grid=self.param_grid, cv=5, n_jobs=-1)
            #Searching the best parameter
            self.grid.fit(train_x,train_y)

            #extracting the best parameters
            self.n_estimators = self.grid.best_params_['n_estimators']
            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']
            self.max_features = self.grid.best_params_['max_features']
            self.logger.info("RandomForestClassifier best parameters: "+str(self.grid.best_params_))

            #creating a new tuned model with the best parameters
            self.rfc = RandomForestClassifier(n_estimators=self.n_estimators, criterion=self.criterion,
                                              max_depth=self.max_depth, max_features=self.max_features)

            #Training the new model
            self.rfc.fit(train_x, train_y)
            self.logger.info("Created a tuned RandomForestClassifier model.")
            self.logger.info("End of finding the best parameters for RandomForestClassifier.")
            return  self.rfc
        except Exception as e:
            self.logger.exception("Error occurred while finding best parameters for RandomForestClassifier")
            raise Exception()

    def best_params_xgboost(self, train_x, train_y):
        try:
            self.logger.info("Starting to find the best parameters for XGBClassifier.")
            #Parameter Grid
            self.param_grid = {
                'n_estimators': [50, 100, 120, 150, 180, 200],
                'max_depth' : list(range(2,11)),
                'learning_rate' : [0.001,0.01,0.1,0.5,1,1.5,2],
                'booster' : ['gbtree', 'gblinear','dart']
            }
            #Instantiating the GridSearchCV
            self.grid = GridSearchCV(estimator=self.xgb, param_grid=self.param_grid, cv = 5, n_jobs=-1)
            #Finding the best params
            self.grid.fit(train_x, train_y)

            #extracting the best parameters
            self.n_estimators = self.grid.best_params_['n_estimators']
            self.learning_rate = self.grid.best_params_['learning_rate']
            self.max_depth = self.grid.best_params_['max_depth']
            self.booster = self.grid.best_params_['booster']
            self.logger.info("Best parameters for XDBClassifier: "+str(self.grid.best_params_))

            #Creating a new tuned model
            self.xgb = XGBClassifier(n_estimators=self.n_estimators, learning_rate=self.learning_rate,
                                     max_depth=self.max_depth, booster=self.booster, objective='binary:logistic')
            self.xgb.fit(train_x,train_y)
            self.logger.info("End of finding best param for XGBClassifier.")
            return self.xgb
        except Exception as e:
            self.logger.exception("Error occurred while finding best parameters for XGBClassifier. %s" %e)
            raise Exception()


    def get_best_model(self, train_x, train_y, test_x, test_y):
        try:
            self.logger.info("Starting get best model.")
            #Creating the best model for XGBClassifier
            self.xgboost = self.best_params_xgboost(train_x,train_y)
            self.prediction_xgboost = self.xgboost.predict(test_x)   #Predictions using xgboost.


            #Creating the best model for RandomForestClassifier
            self.random_forest = self.best_params_randomforest(train_x,train_y)
            self.prediction_random_forest = self.random_forest.predict(test_x)

            #Choose the score to use.
            #If there is only one label in y, then ROC returns error. In that case we will usr accuracy
            if len(test_y.unique()) == 1:
                self.xgboost_score = accuracy_score(test_y, self.prediction_xgboost)
                self.random_forest_score = accuracy_score(test_y, self.prediction_random_forest)
                self.logger.info("Accuracy for XGBOOST: "+str(self.xgboost_score))
                self.logger.info("Accuracy for Random Forest: " + str(self.random_forest_score))
            else:
                self.xgboost_score = roc_auc_score(test_y, self.prediction_xgboost)
                self.random_forest_score = roc_auc_score(test_y, self.prediction_random_forest)
                self.logger.info("ROC AUC score for XGBOOST: "+str(self.xgboost_score))
                self.logger.info("ROC AUC score for Random Forest: " + str(self.random_forest_score))

            #Comparing the model scores
            if self.random_forest_score > self.xgboost_score:
                return 'RandomForest',self.random_forest
            else:
                return 'XGBoost', self.xgboost
        except Exception as e:
            self.logger.exception("Error occurred while getting the best model. %s" %e)
            raise Exception()