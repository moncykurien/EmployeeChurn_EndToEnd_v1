from wsgiref import simple_server
from flask import Flask, request, render_template
from flask_cors import CORS, cross_origin
from flask import Response
import flask_monitoringdashboard as dashboard

import pandas as pd

from apps.core import config
from apps.core.config import Config
from apps.training.train_model import TrainModel
from apps.prediction.predict_model import PredictModel

app = Flask(__name__)
CORS(app)
dashboard.bind(app)

@app.route('/', methods=['POST','GET'])
def index_page():
    """
    * method: index_page
    * description: method to call index html page
    * return: index.html
    *
    * who             when           version  change (include bug# if apply)
    * ----------      -----------    -------  ------------------------------
    * moncykurien      05-MAY-2020    1.0      initial creation
    *
    * Parameters
    *   None
    """

    return render_template('index.html')


@app.route('/training', methods=['POST'])
@cross_origin()
def training_route_client():
    """
    * method: training_route_client
    * description: method to call training route
    * return: none
    *
    * who             when           version  change (include bug# if apply)
    * ----------      -----------    -------  ------------------------------
    * moncykurien       05-MAY-2020    1.0      initial creation
    *
    * Parameters
    *   None
    """
    try:
        #initiate the Config class
        config = Config()
        #get run_id
        run_id = config.get_run_id()
        #Get training data file path
        data_path = config.training_data_path
        #initiate TrainModel object
        trainModel = TrainModel(run_id, data_path)
        #Training the model with training data
        trainModel.training_model()
        return Response("Training Successfull! and its Run Id is %s" %str(run_id))
    except ValueError:
        return Response("Error Occurred! %s " %ValueError )
    except KeyError:
        return Response("Error Occurred! %s " %KeyError )
    except Exception as e:
        return Response("Error Occurred! %s " %e )


@app.route('/batchprediction', methods=['POST'])
@cross_origin()
def batch_prediction_route_client():
    """
    * method: batch_prediction_route_client
    * description: method to call batch prediction route
    * return: none
    *
    * who             when           version  change (include bug# if apply)
    * ----------      -----------    -------  ------------------------------
    * bcheekati       05-MAY-2020    1.0      initial creation
    *
    * Parameters
    *   None
    """
    try:
        #initiate the config class
        config = Config()
        #get run id and data path from the config class
        run_id = config.get_run_id()
        data_path = config.prediction_data_path
        make_predictions = PredictModel(run_id, data_path)
        make_predictions.batch_predict_from_model()
        return Response("Prediction Successfull! and its Run Id is %s" %str(run_id))
    except ValueError:
        return Response("Error Occurred! %s " %ValueError )
    except KeyError:
        return Response("Error Occurred! %s " %KeyError )
    except Exception as e:
        return Response("Error Occurred! %s " %e )


@app.route('/prediction', methods=['POST'])
@cross_origin()
def single_prediction_route_client():
    try:
        config = Config()
        run_id = config.get_run_id()
        data_path = config.prediction_data_path

        if request.method == 'POST':

            satisfaction_level = request.form["satisfaction_level"]
            last_evaluation = request.form["last_evaluation"]
            number_project = request.form["number_project"]
            average_montly_hours= request.form["average_montly_hours"]
            time_spend_company= request.form["time_spend_company"]
            Work_accident = request.form["Work_accident"]
            promotion_last_5years= request.form["promotion_last_5years"]
            salary= request.form["salary"]

            data = pd.DataFrame(
                data = [[0, satisfaction_level, last_evaluation, number_project, average_montly_hours,
                         time_spend_company, Work_accident, promotion_last_5years, salary]],
                columns = ['empid', 'satisfaction_level', 'last_evaluation', 'number_project', 'average_montly_hours',
                           'time_spend_company', 'Work_accident', 'promotion_last_5years', 'salary'])

            convert_dict = {'empid' : int,
                            'satisfaction_level' : float,
                            'last_evaluation' : float,
                            'number_project' : int,
                            'average_montly_hours' : int,
                            'time_spend_company' : int,
                            'Work_accident' : int,
                            'promotion_last_5years' : int,
                            'salary': object}
            data = data.astype(convert_dict)
            predict_model = PredictModel(run_id, data_path)
            output = predict_model.single_predict_from_model(data)
            print('output: '+str(output))
            return Response("Predicted output is: "+str(output))
    except ValueError:
        return Response("Error Occurred! %s " %ValueError )
    except KeyError:
        return Response("Error Occurred! %s " %KeyError )
    except Exception as e:
        return Response("Error Occurred! %s " %e )


if __name__ == '__main__':
    #apps.run()
    host = '0.0.0.0'
    port = 5000
    httpd = simple_server.make_server(host, port, app)
    httpd.serve_forever()