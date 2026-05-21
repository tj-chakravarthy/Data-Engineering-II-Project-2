#!/bin/bash

docker build -t andreashadjoullis1153/crawler:latest .
docker login
docker push andreashadjoullis1153/crawler:latest
