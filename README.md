# MTA-Subway-IoT-Project

An Internet of Things project involving working with NYC MTA subway data to predict fastest time taken to reach a destination between choosing to take the local or the express train, from 42nd st to 96th st and vice versa.

1. The project involves gathering data from the mta subway using an intel edison processor.
2. The data is further uploaded onto Amazon Web Services (AWS) dynamoDB platform to view and observe the data. This is done in    Python using the boto/boto3 library.
3. The data is then filtered and cleaned for data analysis using python and the pandas library.
4. Once cleaned, the data is sent as a csv file to AWS S3 for storage.
5. The data is then analyzed by setting up the relevant attributes needed for analysis and prediction on AWS Machine Learning    platform using linear regression to either take the express or the local train.
6. AWS Lambda is also set up to oversee and automate this process.
7. Finally, AWS SNS (subscription notification service) is used to indicate to the user at any time, based on the model and data, whether to take the local/express train.
