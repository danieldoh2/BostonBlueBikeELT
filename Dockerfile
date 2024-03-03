FROM python:latest


WORKDIR /app
ARG CACHEBUST=1 
COPY . .
RUN mkdir -p /app/blue_bike_data
RUN pip3 install requests beautifulsoup4 pandas sqlalchemy

LABEL maintainer="danieldoh.inbox@gmail.com" 

#For some reason, it takes super long for the python container to start
# CMD [ "sh", "-c", "echo 'Starting the application...' && python ./main.py" ]

CMD [ "python", "./main.py" ]
