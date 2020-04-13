#!/bin/bash

sudo docker run -d -p 27017:27017 -v ~/mongodb_driver:/data/db --name mymongo mongo:latest
sudo docker exec -it mymongo bash