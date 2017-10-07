import pandas as pd
import requests
import re
import sys
from lxml import etree

def get_tables(url):
    req = requests.get(url)
    urls=re.findall('<td scope="row"><a href="(.*?)">', req.text)
    for i in urls:
    	if '10q' in i:
    		url=i
    		print(i)
    url = 'https://www.sec.gov' + url
    print(url)
    html = requests.get(url).text
    data = pd.read_html(html, header=0)

    txt=etree.HTML(html)
    par=txt.xpath('//table')
    for i,k in enumerate(par):
        if str(k.get('border'))=='1':
            df=pd.DataFrame(data[i])
            df.ix[-1]=pd.Series(['']*14)
            print(df.iloc[-1])
            df.to_csv('res.csv',mode='a',encoding='utf-8',index=False)


# get_tables('https://www.sec.gov/Archives/edgar/data/51143/000005114313000007/0000051143-13-000007-index.html')
if __name__ == "__main__":
    CIK = sys.argv[1]
    YYY = sys.argv[2]
    get_tables('https://www.sec.gov/Archives/edgar/data/{}/{}/{}-{}-{}-index.htm'.format(CIK, YYY, YYY[:10],YYY[10:12], YYY[12:]))