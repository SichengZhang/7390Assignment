import pandas as pd
from pandas import DataFrame, read_csv
import urllib
import zipfile
import numpy as np
#import scipy
import sys
import requests
import time
import matplotlib.pyplot as plt
import matplotlib
from scipy.stats import mode
from scipy import stats
from collections import Counter
from matplotlib import cm

class analyze_missingdata:
    # constructor
    def __init__(self, year):
        self.__year = year
        self.__metrics=DataFrame()
        self.__cik = DataFrame()

    # get the file by year
    def getFile(self, year):
        
        ml=[]
        log_list=[]
        for month in range(1, 13):
            if month < 4:
                qtr = "Qtr1"
                m = "0" + str(month)
            elif month < 7 and month > 3:
                qtr = "Qtr2"
                m = "0" + str(month)
            elif month < 10 and month > 6:
                qtr = "Qtr3"
                m = "0" + str(month)
            elif month < 13 and month > 9:
                qtr = "Qtr4"
                m = str(month)
            url = "http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/%s/%s/log%s%s01.zip" % (year, qtr, year, m)
            filename = "log%s%s01.csv" % (year, m)
            urllib.request.urlretrieve(url, filename="log%s%s01.zip" % (year, m))
            zip = zipfile.ZipFile("./log%s%s01.zip" % (year, m), 'r')
            zip.extractall("./")
            c = pd.read_csv(filename, sep=",")
            print (c.head())
            self.analyze(c,m)
            ml.append(self.__metrics)
            self.__metrics = DataFrame()
            log_list.append(filename)
        #self.__cik.plot()
        plt.show()    
        summary_year = self.metrics_year(ml)
        summary_year.to_csv("%s-Summary.csv"%self.__year)
        self.combine_data(log_list)

    # store all the log information in a file
    def log(self, information):
        print("writing log")
        with open("logfile", "a") as f:
            f.write(information)

     
    

    def metrics_month(self, df):
        print("combining month metrics")
        if self.__metrics.empty:
            self.__metrics = df
        else :
            self.__metrics = self.__metrics.join(df)

             
    
    def metrics_year(self, ml):
        print("combining year metrics")
        my = DataFrame()
        my = pd.concat(ml)

        return my

    def cik_year(self, c):
        if self.__cik.empty:
            self.__cik = c
        else :
            self.__cik = pd.concat([self.__cik, c])


    def combine_data(self,log_list):
        print("combining data")
        # fout = open("data_%s.csv"%self.__year,"a")


        # for line in open ("log%s0101.csv"%self.__year):
        #     fout.write(line)
        # month={"01","02","03","04","05","06","07","08","09","10","11","12"}
        # for m in month:
        #     f = open ("log%s%s01.csv"%(self.__year, m))
        #     f.readline()
        #     for line in f:
        #         fout.write(line)
        #     f.close()
        # fout.close()

        pieces = []
        for f in log_list:
            df = pd.read_csv(f)
            pieces.append(df)

        merged_data = pd.concat(pieces, keys=log_list)
        final_data = "%s-final-data.csv"%self.__year
        merged_data.to_csv(final_data)

        return final_data

    # def is_valid_ipv4_address(self, address):
    #     try:
    #         socket.inet_pton(socket.AF_INET, address)
    #     except AttributeError:  # no inet_pton here, sorry
    #         try:
    #             socket.inet_aton(address)
    #         except socket.error:
    #             return False
    #         return address.count('.') == 3
    #     except socket.error:  # not a valid address
    #         return False

    #     return True

    # def is_valid_ipv6_address(self, address):
    #     try:
    #         socket.inet_pton(socket.AF_INET6, address)
    #     except socket.error:  # not a valid address
    #         return False
    #     return True
    def validate_ip(self, s):
        a = s.split('.')
        if len(a) != 4:
            return False
        for x in a[:3]:
            if not x.isdigit():
                return False
            i = int(x)
            if i < 0 or i > 255:
                return False
        return True

    # do analyze for each column
    def analyze(self, f,m):
        print("analyzing data of %s/%s/01"%(self.__year,m))
        self.log("analyzing data of %s/%s/01\n"%(self.__year,m))
        self.analyze_colip(f["ip"])
        self.analyze_coltime(f["time"])
        self.analyze_colcode(f["code"])
        self.analyze_colzone(f["zone"])
        self.analyze_coldate(f["date"],m)
        self.analyze_colidx(f["idx"])
        self.analyze_colcik(f["cik"],m)
        self.analyze_colaccession(f["accession"])
        self.analyze_coldoc(f["extention"])
        self.analyze_colfilesize(f["size"])
        self.analyze_colnorefer(f["norefer"])
        self.analyze_colnoagent(f["noagent"])
        self.analyze_colfind(f["find"])
        self.analyze_colcrawler(f["crawler"], m)
        self.analyze_colbrowser(f["browser"], m)

    # do analyze for column IP
    def analyze_colip(self, f):
        print("analyzing ip....")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        print(missing_counts)
        f.dropna()
        f.fillna(f.sample())
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in IP. \n"
        else:
            information = time.ctime() + ": There are %s missing data in IP. Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        self.log(information)
        valid_ipadd_counts = 0
        invalid_ipadd_counts = 0
        for ip in f :
            if self.validate_ip(ip):
                valid_ipadd_counts +=1
            else :
                invalid_ipadd_counts += 1
        self.log("%s valid ip address and %s invalid ip address found\n"%(valid_ipadd_counts, invalid_ipadd_counts))
        ipdf = {"valid ip address" : [valid_ipadd_counts], "invalid ip address" : [invalid_ipadd_counts]}
        x = DataFrame(ipdf)
        print(x)
        self.metrics_month(x)
        self.log("merging monthly data with other columns\n")


    # do analyze for column date
    def analyze_coldate(self, f,m):
        print("analyzing date....")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        f.dropna()
        f.fillna(year + m + "01")
        f.replace('\*', year + m + "01")
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing datain date.\n "
        else:
            information = time.ctime() + ": There are %s missing data in date. Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        self.log(information)
        dat = {"date": ["%s/%s/01"%(self.__year, m)]}
        x = DataFrame(dat)
        
        self.metrics_month(x)
        self.log("merging monthly data with other columns\n")


    def analyze_colcode(self, f):
        print("analyzing accession....")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        f.dropna()
        f.fillna(f.sample())
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in accession. \n"
        else:
            information = time.ctime() + ": There are %s missing data in accession. Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        # calculate the most viewed company
        self.log(information)

        x = f.value_counts()
        #self.metrics_month(x)
        self.log("merging monthly data with other columns\n")

        count = 0
        rows = []
        for i in f:
            if int (i) == 304 or int (i) == 404 or int (i) == 500 or int (i) == 400:
               count+=1
               rows.append(count)
        if count != 0:
            abnormal = "%s abnormal code caught at rows: %s from column code. "%(count, rows)
            self.log(abnormal)


    # do analyze for column time
    def analyze_coltime(self, f):
        print("analyzing time....")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        f.dropna()
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in time. \n"
        else:
            information = time.ctime() + ": There are %s missing data in time. Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        self.log(information)
        period3_6 = 0
        period7_10 = 0
        period11_14 = 0
        period15_18 = 0
        period19_22 = 0
        period23_2 = 0
        for t in f :
            
            a = t.split(":")
            
            if int(a[0]) > 2 and int(a[0]) < 7:
                period3_6 += 1
            elif int(a[0]) > 6 and int(a[0]) < 11:
                period7_10 += 1
            elif int(a[0]) > 10 and int(a[0]) < 15:
                period11_14 += 1
            elif int(a[0]) > 14 and int(a[0]) < 19:
                period15_18 += 1
            elif int(a[0]) > 18 and int(a[0]) < 23:
                period19_22 += 1
            elif int(a[0]) > 22 and int(a[0]) < 25 :
                period23_2 += 1
            elif int(a[0]) < 3:
                period23_2 += 1

        period = {"3:00-7:00" : [period3_6], "7:00-11:00" : [period7_10], "11:00-15:00": [period11_14], "15:00-19:00" : [period15_18], "19:00-23:00" : [period19_22], "23:00-3:00" : [period23_2]}
        x = DataFrame(period)
        print (x)
        self.metrics_month(x)
        self.log("merging monthly data with other columns\n")

    # do analyze for column zone
    def analyze_colzone(self, f):
        print("analyzing zone....")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in zone.\n"
        else:
            information = time.ctime() + ": There are %s missing data in zone.Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        self.log(information)
        x = f.value_counts()
        data = Counter(f)
        z = data.most_common(1)
        y={"zone" : [z]}
        a = DataFrame(y)
        self.metrics_month(a)        
        self.log("merging monthly data with other columns\n")

    # do analyze for column cik
    def analyze_colcik(self, f, m):
        print("analyzing cik....")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        f.dropna()
        f.fillna(f.sample())
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in cik. \n"
        else:
            information = time.ctime() + ": There are %s missing data in cik.Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        # calculate the most viewed company
        
        # pass cik to information
        # self.log(information)
        # plt.figure("Mothly Access Number--%s"%m)
        x = f.value_counts()
        

        cik1 = x.index[0]
        cik2 = x.index[1]
        cik3 = x.index[2]
        cik1num = x[cik1]
        cik2num = x[cik2]
        cik3num = x[cik3]

        q= {cik1:[cik1num],cik2:[cik2num],cik3:[cik3num]}
        w = DataFrame(q)
        
        #self.cik_year(w)


        # w = DataFrame(q)
        # e = w.set_index(["%s/%s/01"%(self.__year, m)])


        

        #analyze cik with time plot
        #self.metrics_month(x)
        self.log("merging monthly data with other columns\n")

    #do analyze for column accession
    def analyze_colaccession(self, f):
        print("analyzing accession....")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        f.dropna()
        f.fillna(f.sample())
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in accession. \n"
        else:
            information = time.ctime() + ": There are %s missing data in accession. Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        # calculate the most viewed company
        self.log(information)

        x = f.value_counts()
        #self.metrics_month(x)
        self.log("merging monthly data with other columns\n")
    
    #do analyze for column filesize
    def analyze_colfilesize(self, f):
        print("analyzing filesize....")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        f.dropna()
        f.fillna(value=0)
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in filesize. \n"
        else:
            information = time.ctime() + ": There are %s missing data in filesize.Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        self.log(information)

        
        count = 0
        rows = []
        for i in f:
            if float(i) < 1000:
               count+=1
               rows.append(count)
        if count != 0:
            abnormal = "%s abnormal file size caught at rows: %s from column filesize. "%(count, rows)
            self.log(abnormal)
            
        self.log("merging monthly data with other columns\n")

    def analyze_coldoc(self, f):
        print("analyzing doc....")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        f.dropna()
        f.fillna(f.sample())
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in doc. \n"
        else:
            information = time.ctime() + ": There are " + str(missing_counts) + " missing data in doc.Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        self.log(information)

        data = Counter(f)
        z = data.most_common(1)
        y={"extension" : [z]}
        a = DataFrame(y)
        self.metrics_month(a)
        self.log("merging monthly data with other columns\n")
        
    #do analyze for column norefer
    def analyze_colnorefer(self, f):
        print("analyzing norefer....")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        f.dropna()
        f.fillna(f.sample())
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in norefer. \n"
        else:
            information = time.ctime() + ": There are %s missing data in norefer.Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        self.log(information)
        x = f.value_counts()

        if x.index.size > 1 :
            p = x.index[0]
            q = x.index[1]
            isrefer = x[p]
            norefer = x[q]
            refered={"isrefer" : ["1"]}
            norefered={"isrefer" : ["0"]}
            if int(isrefer) > int(norefer):
                y = DataFrame(refered)
                self.metrics_month(y)
            else :
                y = DataFrame(refered)
                self.metrics_month(y)
        else :
            p = x.index[0]
            isagent = x[p]
            refered={"isrefer" : ["1"]}
            y = DataFrame(refered)
            self.metrics_month(y)    

        count = 0
        rows = []
        for i in f:
          if int(i) != 0 and int(i) != 1:
               count+=1
               rows.append(count)
        if count != 0:
            abnormal = "%s abnormal data caught at rows: %s from column idx. "%(count, rows)
            self.log(abnormal)
        self.log("merging monthly data with other columns\n")

    # do analyze for column idx
    def analyze_colidx(self, f):
        print("analyzing index......")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        sum = f.shape
        x = f.value_counts()
        
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing datain idx. \n"
        else:
            information = time.ctime() + ": There are %s missing datain idx. Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        self.log(information)    

        count = 0
        rows = []
        for i in f:
          if int(i) != 0 and int(i) != 1:
               count+=1
               rows.append(count)
        if count != 0:
            abnormal = "%s abnormal data caught at rows: %s from column idx. "%(count, rows)
            self.log(abnormal)
        
        

        x = f.value_counts()
        self.metrics_month(x)
        self.log("merging monthly data with other columns\n")
        print("index analyzing finished......")
    # do analyze for column agent
    def analyze_colnoagent(self, f):
        print("analyzing agent......")
        f.dropna()
        f.fillna("1")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        number = f.shape
        x = f.value_counts()
        print(x)
        if x.index.size > 1 :
            p = x.index[0]
            q = x.index[1]
            isagent = x[p]
            noagent = x[q]
            agented={"isagent" : ["1"]}
            noagented={"isagent" : ["0"]}
            abc = str(isagent) + " users are using agent, and " + str(noagent) + " users are not. \n"
            self.log(abc)
            if int(isagent) > int(noagent):
                
                y = DataFrame(agented)
                self.metrics_month(y)
            else :
                
                y = DataFrame(noagented)
                self.metrics_month(y)
        else :
            #print ("12345678")
            p = x.index[0]
            isagent = x[p]
            noagented={"isagent" : ["0"]}
            y = DataFrame(noagented)
            self.metrics_month(y)        

        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in noagent. \n"
        else:
            information = time.ctime() + ": There are %s missing data in noagent. Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        
        self.log(information)

        count = 0
        rows = []
        for i in f:
          if int(i) != 0 and int(i) != 1:
               count+=1
               rows.append(count)
        if count != 0:
            abnormal = "%s abnormal data caught at rows: %s from column idx. "%(count, rows)
            self.log(abnormal)

        
        
        self.log("merging monthly data with other columns\n")

    #do analyze for column find
    def analyze_colfind(self, f):

        missing_counts = f.ix[pd.isnull(f),].shape[0]
        f.dropna()
        f.fillna(f.sample())
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in find. \n"
        else:
            information = time.ctime() + ": There are %s missing data in find. Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        self.log(information)
        x = f.value_counts()

        data = Counter(f)
        z = data.most_common(1)
        y={"find" : [z]}
        a = DataFrame(y)
        self.metrics_month(a)
        self.log("merging monthly data with other columns\n")

    #do analyze for column crawler
    def analyze_colcrawler(self, f, m):

        missing_counts = f.ix[pd.isnull(f),].shape[0]
        f.dropna()
        f.fillna(f.sample())
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in crawler. \n"
        else:
            information = time.ctime() + ": There are %s missing data in crawler. Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        self.log(information)
        x = f.value_counts()
        
        if x.index.size > 1 :
            p = x.index[0]
            q = x.index[1]
            zero = x[p]
            one = x[q]
            zeroS={"crawler" : ["1"]}
            oneS={"crawler" : ["0"]}
            if int(zero) > int(one):
                y=DataFrame(zeroS)
                self.metrics_month(y)
               

            else :
                y=DataFrame(oneS)
                self.metrics_month(y)
        else :
            p = x.index[0]
            isagent = x[p]
            oneS={"crawler" : ["0"]}
            y=DataFrame(oneS)
            self.metrics_month(y)    
        self.log("merging monthly data with other columns\n")

        # cs=cm.Set1(np.arange(40)/40.)
        # ax=d.add_subplot(111, aspect='equal')
        plt.figure("Crawler using distribution--%s"%m)
        x.plot.pie(labels=["using crawler","not using crawler"], autopct='%.2f', fontsize=9, figsize=(5, 5))

    # do analyze for column brw
    def analyze_colbrowser(self, f, m):
        print("analyzing browser......")
        missing_counts = f.ix[pd.isnull(f),].shape[0]
        # shoutcut_to_browser = {"mie":"MSIE", "fox":"Firefox","saf":"Safari",
        # "chr":"Chrom", "sea":"Seamonk", "opr":"Opera",
        # "oth":"DoCoMo|KDDI|Cricket|Vodaphone",
        # "win":"windows", "mac":"Mac", "lin":"Linux",
        # "iph":"iPhone", "ipd":"iPad", "and":"Android",
        # "rim":"BB10|PlayBook|BlackBerry",
        # "iem":"IEMobile|Windows\s*CE|Windows\s*Phone"}
        sum = f.shape
        #metrics
        x = f.value_counts()
        data = Counter(f)
        z = data.most_common(1)
        y={"browser" : [z]}
        a = DataFrame(y)
        self.metrics_month(a)
        information = ""
        if missing_counts == 0:
            information = time.ctime() + ": no missing data in browser. \n"
        else:
            information = time.ctime() + ": There are %s missing data in browser. Located at rows %s\n" % (missing_counts,f.ix[pd.isnull(f),].index)
        self.log(information)

        #draw pic

        d = plt.figure("Monthly Top Popular Browser--%s"%m)
        # cs=cm.Set1(np.arange(40)/40.)
        # ax=d.add_subplot(111, aspect='equal')
        x.plot.pie(labels=x.index, autopct='%.2f', fontsize=9, figsize=(5, 5),startangle=90,shadow=True)
        
        self.log("merging monthly data with other columns\n")
    # combine 12 data files and summaries
   # def combine(self, ):


# main function

year = sys.argv[1]

abc = analyze_missingdata(year)
abc.getFile(year)
