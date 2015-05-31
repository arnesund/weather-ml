# weather-ml
Experiment with Machine Learning on weather datasets: Can Amazon Machine Learning be used to predict the weather?

For a walkthrough of how to perform the analysis, and some results, see my blog post at http://arnesund.com/2015/05/31/using-amazon-machine-learning-to-predict-the-weather/

## Prerequisites

 - API key from wunderground.com
 - Amazon AWS account

## How to use

 1. Ensure API key is in environment with "export WUNDERGROUND_APIKEY=..."
 2. Edit the list of cities in generate_dataset.py if you'd like
 3. Run ./generate_dataset.py without arguments
 4. Upload data/dataset.csv and data/testset.csv to Amazon S3
 5. Use dataset.csv to create a ML model at Amazon Machine Learning
 6. Use testset.csv as the input for batch prediction on the new ML model
