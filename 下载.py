import requests
import pandas as pd
import csv

# 站点编号读取
# 读取CSV文件
file_path = r'C:\Users\13RNR-i9\Desktop\并行\3\水质测定点位置数据.csv'
df = pd.read_csv(file_path)

# 提取第三列数据
zettaicode_column = df['zettaicode']

# 将第三列数据转换成字符串数组
zettaicode_strings = zettaicode_column.apply(lambda x: str(x).zfill(7))
site_codes = zettaicode_strings
# 年份范围
years = range(1984, 2022+1)
#years = range(2021, 2022+1)

# 标志，指示何时开始处理，处理内核死亡后继续下载的情况。
start_processing = False

with open('C:/Users/13RNR-i9/Desktop/并行/3/error.csv', 'w', newline='') as csvfile:
    # 创建CSV写入对象
    csvwriter = csv.writer(csvfile)


    for site_code in site_codes:
        if site_code == '3510520':
            start_processing = True

        if site_code == '3530400':
            start_processing = False

        if start_processing:
            for year in years:
                zettaicode=site_code
                yearS=year
                # 构造URL
                # 第一步,请求原始网页
                url=f'https://water-pub.env.go.jp/water-pub/mizu-site/mizuyear/win_AllDetail.asp?zettaicode={zettaicode}&yearS={yearS}'
                response = requests.get(url)
                print(url)

                #检查是否存在站点
                target_string = '該当する地点はありません。下の条件を変更して下さい。'  # 要检查的目标字符串

                try:
                    response = requests.get(url)
                    response.encoding='shift_jis'  # 使用正确的 SHIFT JIS 编码
                    if response.status_code == 200:
                        # 成功获取网页内容
                        page_content = response.text
                        if target_string in page_content:
                            print(f'{zettaicode}站点的{yearS}年数据不存在')
                        else:
                            print(f'正在保存{zettaicode}站点的{yearS}年数据')

                            cookies = response.cookies
                            # 构造下载请求
                            download_data = {
                            'zettaicode': zettaicode,
                            'yearS': yearS  
                            }

                            download_headers = {
                            'User-Agent': 'Mozilla/5.0',
                            }

                            # 用原网页的 cookies 请求下载接口
                            download_response = requests.post(
                            'https://water-pub.env.go.jp/water-pub/mizu-site/mizuyear/win_AllDetailDownLoad.asp',
                            data=download_data,
                            headers=download_headers,
                            cookies=cookies  
                            )

                            # 获取文件名并保存
                            filename = download_response.headers['Content-Disposition'].split('=')[1]
                            with open(f'C:/Users/13RNR-i9/Desktop/并行/3/{zettaicode}_{yearS}_ALL.csv', 'wb') as f:
                                f.write(download_response.content)
                    else:
                        print('无法获取网页，状态码：', response.status_code)
                except requests.exceptions.RequestException as e:
                    print('发生网络请求异常：', e)
                    csvwriter.writerow(site_code+'_'+str(year)) 

print('所有网页处理完毕')

