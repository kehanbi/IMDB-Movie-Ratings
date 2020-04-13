import sys
import socket
import time
from bs4 import BeautifulSoup
from tqdm import tqdm
import datetime
import requests
import os
import pandas as pd
from lxml import etree

class IMDB(object):
    """
    init class imdb
    """
    def __init__(self):
        self.dataset_location = os.path.realpath(os.path.join(os.path.dirname(__file__), "DataSets"))
        self.current_year = int(datetime.datetime.now().year)

    def crawl_ratings(self):
        write_header = True
        headers= {'User-agent': 'Mozilla/5.0'}
        input_year = int(input("Please enter start year (eg. 2019): "))
        if input_year > self.current_year:
            print("No movie is recorded in year %i yet!!" % (input_year))
        else:
            for year in tqdm(range(input_year, self.current_year+1)):
                #sys.stdout = open(os.path.join(dataset_location,"IMDB_Top50_" + str(year) + '.txt'), 'w+')
                url = "http://www.imdb.com/search/title?release_date=" + str(year) + "," + str(year) + "&title_type=feature"
                print (url)
                response = requests.get(url,headers=headers)
                soup = BeautifulSoup(response.text, "html.parser")
                article = soup.find('div', attrs={'class': 'article'}).find('h1')
                print (article.contents[0] + ': ')
                lister_list_contents = soup.find('div', attrs={'class': 'lister-list'})
                i = 1
                #找到所有：显示一个movie的div
                movieList = soup.findAll('div', attrs={'class': 'lister-item mode-advanced'})
                response.close()

                timeout = 20
                socket.setdefaulttimeout(timeout)  # 这里对整个socket层设置超时时间。后续文件中如果再使用到socket，不必再设置
                sleep_download_time = 180

                for div_item in tqdm(movieList):
                    div = div_item.find('div', attrs={'class': 'lister-item-content'})
                    print (str(i) + '.',)
                    header = div.findChildren('h3', attrs={'class': 'lister-item-header'})
                    a = (header[0].findChildren('a'))[0];
                    a_href = a['href']
                    alist = a_href.split('?')
                    a_ratings = alist[0]+'ratings?ref_=tt_ov_rt'
                    print(a_ratings);
                    MovieName = str((header[0].findChildren('a'))[0].contents[0].encode('utf-8').decode('ascii', 'ignore'))
                    print ('Movie: ' + MovieName)
                    i += 1
                    if i<=45 : continue

                    time.sleep(sleep_download_time)  # 这里时间自己设定
                    response = requests.get('http://www.imdb.com/' + a_ratings)
                    soup = BeautifulSoup(response.text, "html.parser")
                    title_ratings_sub_page = soup.find('div', attrs={'class': 'title-ratings-sub-page'})
                    ir = soup.find('div',attrs={'name':'ir'});
                    print(ir['data-value'])

                    tables = title_ratings_sub_page.findChildren('table')
                    #print(tables[1])
                    response.close()

                    # 直接使用pandas获取和解析数据
                    data_res = pd.read_html(tables[1].prettify(),header = 0)
                    data = data_res[0]
                    data['title'] = MovieName
                    #print(data)
                    #data.to_csv('a.csv', index=False, mode='a', header=True)

                    header = ['title', 'Demographic', 'All Ages', '<18', '18-29', '30-44', '45+', 'All Ages Voters',
                              '<18 Voters', '18-29 Voters', '30-44 Voters', '45+ Voters']
                    #data.rename(columns={'Demographic': '0'}, inplace=True)

                    df_1 = pd.DataFrame(data=None,columns=header)
                    df_1['title'] = data['title']
                    df_1['Demographic'] = data.iloc[:,0]
                    df = data["All Ages"].str.split(expand=True)
                    df_1['All Ages'] = df[0]
                    df_1['All Ages Voters'] = df[1]
                    df = data["<18"].str.split(expand=True)
                    df_1['<18'] = df[0]
                    df_1['<18 Voters'] = df[1]
                    df = data["18-29"].str.split(expand=True)
                    df_1['18-29'] = df[0]
                    df_1['18-29 Voters'] = df[1]
                    df = data["30-44"].str.split(expand=True)
                    df_1['30-44'] = df[0]
                    df_1['30-44 Voters'] = df[1]
                    df = data["45+"].str.split(expand=True)
                    df_1['45+'] = df[0]
                    df_1['45+ Voters'] = df[1]

                    if write_header is True:
                        df_1.to_csv('a.csv', index=False, mode='a', header=True)
                        write_header = False
                    else:
                        df_1.to_csv('IMDB_Data.csv', index=False, mode='a', header=False)
                    print('Successfully crawled data  and saved it to file!')
                    #break
                    """
                    #data.replace(r'\  ([0-9,]*)', '', regex=True,inplace = True)
                    """
                    #time.sleep(60)

if __name__ == '__main__':
    imdb = IMDB()
    imdb.crawl_ratings()