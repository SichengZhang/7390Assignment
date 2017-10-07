FROM patavee/scipy-py3

RUN pip3 install requests 
RUN pip3 install pandas 
RUN pip3 install numpy 
RUN pip3 install scipy 
RUN pip3 install boto3 
RUN pip3 install matplotlib
RUN pip3 install zipfile
RUN pip3 install collections

COPY ./2.py /src/2.py

CMD ["matplotlib.use('Agg')"]
CMD ["python3", "2.py"]

