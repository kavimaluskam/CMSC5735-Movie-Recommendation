from pyspark import SparkContext, SparkConf
import math
from pyspark.mllib.recommendation import ALS

conf = SparkConf().setAll([
    ('spark.executor.memory', '20g'), 
    ('spark.executor.cores', '4'), 
    ('spark.cores.max', '24'), 
    ('spark.driver.memory','30g'),
    ('spark.driver.cores', '4'), 
    ('spark.executor.instances', '4')
])

sc = SparkContext(conf=conf)
sc.getConf().getAll()
raw_data = sc.textFile("CHANGEME")
data = raw_data.map(lambda line: line.split(',')).map(lambda tokens: (tokens[0], tokens[1], tokens[2])).cache()

#split dataset
training_RDD, validation_RDD, test_RDD = data.randomSplit([6, 2, 2])
validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

#training parameters
iterations = 10
regularization_parameter = 0.1
ranks = [4, 8, 12]
errors = [0, 0, 0]
err = 0
tolerance = 0.02

#training
min_error = float('inf')
best_rank = -1
best_iteration = -1
for rank in ranks:
    model = ALS.train(training_RDD, rank, iterations=iterations,
                      lambda_=regularization_parameter, nonnegative = True)
    predictions = model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
    rates_and_preds = validation_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
    error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    errors[err] = error
    err += 1
    print ('For rank %s the RMSE is %s' % (rank, error))
    if error < min_error:
        min_error = error
        best_rank = rank

best_model = ALS.train(data, best_rank, iterations=iterations,
                      lambda_=regularization_parameter, nonnegative = True)
model.save(sc, "col_filtering_best_model")
