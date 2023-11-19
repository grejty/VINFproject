FROM coady/pylucene
WORKDIR /usr/src/app
RUN pip install pyspark
RUN pip install pandas
COPY . .
CMD ["python3", "-m", "http.server", "80"]

