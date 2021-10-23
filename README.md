# cats user search

search/serving service for users 

service is consuming user domain events from kafka topic and indexing them to elasticsearch

service got gRpc for user search


see also [zio-user-search](https://github.com/justcoon/zio-user-search), [akka-typed-user](https://github.com/justcoon/akka-typed-user)

# required

* elasticsearch
* kafka