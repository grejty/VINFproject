FROM coady/pylucene
WORKDIR /usr/src/app
RUN pip install bs4 
RUN pip install requests 
RUN pip install pandas 
RUN pip install java 
RUN pip install lucene
RUN main.py
COPY . .

