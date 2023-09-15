## Hokudai　Faculty of Env.Earth Science　
## Nie Qiyang 
## 2023.09.12
## utf-8
## 水環境総合情報サイトデーターを自動にダウンロード

import requests
import pandas as pd

# Site number reading站点编号读取
file_path = r'.\Site.csv'
df = pd.read_csv(file_path)

# Extract the third column of data 提取第三列数据
zettaicode_column = df['zettaicode']

# 将第三列数据转换成字符串数组
# Convert the third column of data into a string array
zettaicode_strings = zettaicode_column.apply(lambda x: str(x).zfill(7))
site_codes = zettaicode_strings
# year range 年份范围
years = range(1984, 2022+1)
#years = range(2021, 2022+1)

# 标记，指示何时开始处理，处理内核死亡后继续下载的情况。
# Indicates when to start processing, handling the situation where the download continues after the kernel dies.
start_processing = False
error_list =[]
# Star
for site_code in site_codes:
    if site_code == '3510520':
        start_processing = True
    if site_code == '3530400':
        start_processing = False
    if start_processing:
        for year in years:
            zettaicode=site_code
            yearS=year
            # Construct URL 构造URL
            # The first step is to request the original web page 第一步,请求原始网页
            url=f'https://water-pub.env.go.jp/water-pub/mizu-site/mizuyear/win_AllDetail.asp?zettaicode={zettaicode}&yearS={yearS}'
            response = requests.get(url)
            print(url)

            #Check if the site exists 检查是否存在站点
            target_string = '該当する地点はありません。下の条件を変更して下さい。'  

            try:
                response = requests.get(url)
                response.encoding='shift_jis'  # use SHIFT JIS 
                if response.status_code == 200:
                    # Successfully obtained web content 成功获取网页内容
                    page_content = response.text
                    if target_string in page_content:
                        print(f'{zettaicode}站点的{yearS}年数据不存在')
                    else:
                        print(f'正在保存{zettaicode}站点的{yearS}年数据')

                        cookies = response.cookies
                        # Construct download request 构造下载请求
                        download_data = {
                          'zettaicode': zettaicode,
                          'yearS': yearS  
                        }

                        download_headers = {
                          'User-Agent': 'Mozilla/5.0',
                        }

                        # Use the cookies of the original web page to request the download interface
                        # 用原网页的 cookies 请求下载接口
                        download_response = requests.post(
                          'https://water-pub.env.go.jp/water-pub/mizu-site/mizuyear/win_AllDetailDownLoad.asp',
                          data=download_data,
                          headers=download_headers,
                          cookies=cookies  
                        )

                        # Get the file name and save it 获取文件名并保存
                        filename = download_response.headers['Content-Disposition'].split('=')[1]
                        with open(f'{zettaicode}_{yearS}_ALL.csv', 'wb') as f:
                            f.write(download_response.content)
                else:
                    print('Unable to obtain web page, status code (无法获取网页，状态码：)', response.status_code)
            except requests.exceptions.RequestException as e:
                print('A network request exception occurred (发生网络请求异常：)', e)
                error_list.append(site_code+'_'+str(year))    
        
print('All web pages processed所有网页处理完毕')
print(error_list)
