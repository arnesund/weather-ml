# weather-ml
Experiment with Machine Learning on weather datasets

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
