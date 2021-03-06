// If this file contains double-curly-braces, that's because
// it is a template that has not been processed into JavaScript yet.
console.log('Loading event');
exports.handler = function(event, context) {
  var AWS = require('aws-sdk');
  var sns = new AWS.SNS();
  var ml = new AWS.MachineLearning();
  var endpointUrl = 'https://realtime.machinelearning.us-east-1.amazonaws.com/';
  var mlModelId = 'ml_id2016-03-22-07-16-14';
  var snsTopicArn = 'arn:aws:sns:us-east-1:970128656518:mtaSub';
  var snsMessageSubject = 'Respond to tweet';
  var snsMessagePrefix = 'ML model '+mlModelId+': Respond to this tweet: https://twitter.com/0/status/';
  var numMessagesProcessed = 0;
  var numMessagesToBeProcessed = event.Records.length;
  console.log("numMessagesToBeProcessed:"+numMessagesToBeProcessed);

  var updateSns = function(tweetData) {
    var params = {};
    params['TopicArn'] = snsTopicArn;
    params['Subject']  = snsMessageSubject;
    params['Message']  = snsMessagePrefix+tweetData['sid'];
    console.log('Calling Amazon SNS to publish.');
    sns.publish(
      params,
      function(err, data) {
        if (err) {
          console.log(err, err.stack); // an error occurred
          context.done(null, 'Failed when publishing to SNS');
        }
        else {
          context.done(null, 'Published to SNS');
        }
      }
      );
  }

  var callPredict = function(tweetData){
    console.log('calling predict');
    ml.predict(
      {
        Record : tweetData,
        PredictEndpoint : endpointUrl,
        MLModelId: mlModelId
      },
      function(err, data) {
        if (err) {
          console.log(err);
          context.done(null, 'Call to predict service failed.');
        }
        else {
          console.log('Predict call succeeded');
          if(data.Prediction.predictedLabel === '1'){
              console.log('Switch');
            updateSns(tweetData);
          }
          else{
            context.done(null, "Tweet doesn't require response from customer service");
          }
        }
      }
      );
  }

  var processRecords = function(){
    for(i = 0; i < numMessagesToBeProcessed; ++i) {
      encodedPayload = event.Records[i].kinesis.data;
      // Amazon Kinesis data is base64 encoded so decode here
      payload = new Buffer(encodedPayload, 'base64').toString('utf-8');
      console.log("payload:"+payload);
      try {
        parsedPayload = JSON.parse(payload);
        callPredict(parsedPayload);
      }
      catch (err) {
        console.log(err, err.stack);
        context.done(null, "failed payload"+payload);
      }
    }
  }

  var checkRealtimeEndpoint = function(err, data){
    if (err){
      console.log(err);
      context.done(null, 'Failed to fetch endpoint status and url.');
    }
    else {
      var endpointInfo = data.EndpointInfo;

      if (endpointInfo.EndpointStatus === 'READY') {
        endpointUrl = endpointInfo.EndpointUrl;
        console.log('Fetched endpoint url :'+endpointUrl);
        processRecords();
      } else {
        console.log('Endpoint status : ' + endpointInfo.EndpointStatus);
        context.done(null, 'End point is not Ready.');
      }
    }
  }

  ml.getMLModel({MLModelId:mlModelId}, checkRealtimeEndpoint);
  ml.getMLModel({MLModelId:mlModelId}, checkRealtimeEndpoint);
};

