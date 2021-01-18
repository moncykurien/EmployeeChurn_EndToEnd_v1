# EmployeeChurn_EndToEnd_v1
Employee churn out prediction project. Deployed in AWS EC2 instance.

The project is used to predict if an employee would leave the company or not based on some of the Key indicators.
We can do single data prediction through the web application.

To access the web application hit the following ip address or url
ip : 13.59.2.199
url : http://ec2-13-59-2-199.us-east-2.compute.amazonaws.com/

The application has been deployed in an EC2 ubuntu in AWS. The gateway interface used is Gunicorn.
The loadbalancer/webserver used is NGINX
The project is developed using FLASK framework

For Model retraining approach:
   A new training file in .csv format must be placed in data/training_data folder and then hit the endpoint 13.59.2.199/training
   
For batch prediction:
   The prediction file in .csv format must be place in data/prediction_data folder and then hit the endpoint 13.59.2.199/batchprediction
   
   
