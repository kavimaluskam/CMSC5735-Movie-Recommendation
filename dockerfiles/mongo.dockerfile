FROM mongo

RUN mkdir /db
WORKDIR /db
ADD datasets/mongo /db/datasets/mongo

RUN mongorestore -h db:27017 datasets/mongo/mongodump/ --gzip