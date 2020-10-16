# coding=utf8
import requests,time,random,re,json,datetime,jsonpath,pymysql,os
import setting
from lxml import etree
import schedule
import xlwt
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import mimetypes
from request_proxies import proxies
from multiprocessing import Pool


def request_200(want_url, data=None, req=1, post_headers=None):
    user_agent = random.choice(setting.user_agent_list)
    headers = {
        "authority": "sale.jd.com",
        "method": "GET",
        "path": "/mall/active/43KQGFpu7JT8QYk4jHQJD9rZn1hE/index.html",
        "scheme": "https",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
        # "accept-encoding": "gzip, deflate, br",
        "accept-language": "zh-CN,zh;q=0.9",
        "cache-control": "max-age=0",
        "cookie": '__jdv=122270672|direct|-|none|-|1565171709552; __jdu=15651717095511398966639; shshshfpa=2e7547b8-4a8a-65e7-4158-d6ad066aaa57-1565171709; shshshfpb=svTxBCCtomHkxfy5lqJReQg%3D%3D; areaId=1; ipLoc-djd=1-72-0-0; __jdc=122270672; shshshfp=77a380d2816266a741f7eae37bb28e8a; cid=NXdMNDU2NnRFOTU0N2tONTE4OHpBODU0MXJINjk2MmJTMTkyM3RVODk2NGFKNzU2; pin=h_17349802237; _tp=AvJUiKN3rCfAkfebtDlXfA%3D%3D; _pst=h_17349802237; pinId=QbVYLGtN-GJcroWPoY-I2A; unick=h_173*****237; 3AB9D23F7A4B3C9B=KEATUY7RMH6TIVCAWDGTVDQF22YUSKT3INO3S7XNU2E5WRRMOHN4ARQLVACCIAQGFPZFCSOMG6ZPUFABZPDMI3YL4Y; mba_muid=15651717095511398966639; wlfstk_smdl=keq5kwxxxk53e1asweytliurvz5xx65i; TrackID=1HF19huHmJdOV3NOP94XQ7P50yYDOmd_KOrFX5ZBau6jJ9Pti5pmuVxBFUI2sD_y-tkYUqSt8ip8P9D9sC0eo2g; ceshi3.com=000; __jda=122270672.15651717095511398966639.1565171710.1565688977.1565745474.9; RT="z=1&dm=jd.com&si=68x8gks8ckh&ss=jz7tnycn&sl=0&tt=0&r=ac74bdbed32fbe2b4198b4ab4058fdb0&ul=2rc3l0&hd=2rc3lq"; __jdb=122270672.10.15651717095511398966639|9.1565745474; thor=ECAE387F4272DD4AD8AD80A3B6066A15AEFB2D820EA7665B13943A0AF6B714837A60C484BB106371FB53A6E664609604774306220F3ABC234A5BDD6ABE1F805E8EE25FFAE77175346469A0EB01287072D79C60E1C9889392E8F29F82C49548B481D23492243978F333C27074F7D88FCB63E4F02E2CE17090B02E1065320786591D4BB05ED97CB405F3A06206986F74BA',

        "user-agent": user_agent

    }
    n = 5
    while True:
        try:
            if req == 1:
                want_response = requests.get(want_url, headers=headers, proxies=proxies, timeout=5)
            else:
                want_response = requests.post(want_url, headers=post_headers, proxies=proxies, data=data, timeout=5)
            # print(want_response.text)
            if "502 Bad Gateway" in want_response.text or want_response.status_code != 200 or "您所访问的页面不存在" in want_response.content.decode(
                    'utf-8') or want_response.status_code == 429:
                time.sleep(1)
                continue
            else:
                break
        except Exception as e:
            print('请求异常')
            if '429 To Many Requests' in str(e) or '429 Too Many Requests' in str(e):
                # time.sleep(0.05)
                continue
            if n == 0:
                print("这个链接访问有异常：" + want_url)
                print('error:{0}'.format(e))
                want_response = n
                break
            n -= 1
            time.sleep(1)
            pass

    return want_response


# 专场提醒
def get_remindCount(url):
    response = request_200(url)
    if response == 0:
        return ''
    datas = json.loads(response.text)
    remindCount = jsonpath.jsonpath(datas, '$..remindCount')
    remindCount = remindCount[0] if remindCount else ''
    return remindCount


# 专场
def get_session(session_list):
    while True:
        try:
            conn = pymysql.connect(host='10.10.10.77', user='hanruofei', password='hrf123', db='epai_spider_2019_hanruofei',
                                   charset="utf8")
            cur = conn.cursor()
            break
        except Exception as e:
            print(e)
            time.sleep(2)
    for session_id in session_list:
        url = 'https://paimai.jd.com/json/current/queryCurAlbumInfo?albumIdStr=%s' % session_id
        response = request_200(url)
        if response == 0:
            break
        datas = json.loads(response.text)
        s_name = jsonpath.jsonpath(datas, '$..albumName')
        status = jsonpath.jsonpath(datas, '$..auctionStatus')
        access = jsonpath.jsonpath(datas, '$..access')
        bidCount = jsonpath.jsonpath(datas, '$..bidCount')
        proCount = jsonpath.jsonpath(datas, '$..proCount')
        end_time = jsonpath.jsonpath(datas, '$..endTime')
        s_name = s_name[0] if s_name else ''
        status = status[0] if status else ''
        access = access[0] if access else ''
        bidCount = bidCount[0] if bidCount else ''
        proCount = proCount[0] if proCount else ''
        end_time = end_time[0]/1000 if end_time else ''
        # print(type(end_time))
        if end_time:
            timeArray = time.localtime(end_time)
            end_time = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        crawl_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        # 专场提醒获取
        # remindCount_url = 'https://api.m.jd.com/api?appid=auctionRemind&functionId=queryRemind&body=%7B%22id%22%3A%22{}%22%2C%22remindType%22%3A2%7D'.format(session_id)
        # remindCount = get_remindCount(remindCount_url)
        remindCount = ''
        # print(session_id, s_name, status, access, remindCount, bidCount, crawl_time, proCount, end_time)
        sql = '''INSERT INTO base_collect_sessions
                    ( 
                    SESSION_ID, 
                    SESSION_NAME, 
                    S_STATUS, 
                    S_ONLOOKERS_NUM, 
                    S_REMIND_NUM, 
                    S_OFFER_NUM, 
                    S_COLLECT_TIME, 
                    S_OFFER_ITEM_NUM, 
                    S_END_TIME
                    )
                    VALUES
                    ('%s','%s','%s','%s','%s','%s','%s','%s','%s');

        ''' % (session_id, s_name, status, access, remindCount, bidCount, crawl_time, proCount, end_time)
        try:
            cur.execute(sql)
            conn.commit()
            print('数据成功插入')
        except Exception as e:
            print(e)
            print('数据插入异常')
        # break
    cur.close()
    conn.close()


# 拍品提醒
def get_item_remindCount(i_id):
    url = 'http://api.m.jd.com/api?appid=auctionRemind&functionId=queryRemind&body={"id":%s,"remindType":1}&loginType=3' % i_id
    response = request_200(url)
    if response == 0:
        url = 'https://api.m.jd.com/api?appid=auctionRemind&functionId=queryRemind&body={"id":%s,"remindType":1}&loginType=3' % i_id
        response = request_200(url)
        if response == 0:
            return ''
    datas = json.loads(response.text)
    remindCount = jsonpath.jsonpath(datas, '$..remindCount')
    remindCount = remindCount[0] if remindCount else ''
    return remindCount


# 拍品信息
def get_item_num(i_id):
    url = 'http://api.m.jd.com/api?appid=paimai&functionId=getPaimaiRealTimeData&body={%22end%22:9,%22paimaiId%22:' + i_id + ',%22source%22:0,%22start%22:0}&loginType=3'
    response = request_200(url)
    if response == 0:
        url = 'https://api.m.jd.com/api?appid=paimai&functionId=getPaimaiRealTimeData&body={%22end%22:9,%22paimaiId%22:' + i_id + ',%22source%22:0,%22start%22:0}&loginType=3'
        response = request_200(url)
        if response == 0:
            return '', '', ''
    datas = json.loads(response.text)
    i_access = jsonpath.jsonpath(datas, '$..accessNum')
    i_accessensure = jsonpath.jsonpath(datas, '$..accessEnsureNum')
    i_bid = jsonpath.jsonpath(datas, '$..bidCount')
    i_access = i_access[0] if i_access else ''
    i_accessensure = i_accessensure[0] if i_accessensure else ''
    i_bid = i_bid[0] if i_bid else ''
    return i_access, i_accessensure, i_bid


# 拍品入库
def insert_SQL(session_id, i_id, i_name, i_access, i_accessensure, i_remind, i_bid, crawl_time):
    while True:
        try:
            conn = pymysql.connect(host='10.10.10.77', user='hanruofei', password='hrf123', db='epai_spider_2019_hanruofei',
                                   charset="utf8")
            cur = conn.cursor()
            break
        except Exception as e:
            print(e)
            time.sleep(2)
    sql = '''INSERT INTO base_collect_items
                        ( 
                        COLLECT_SESSION_ID, 
                        JD_ITEM_ID, 
                        I_NAME, 
                        I_ONLOOKERS_NUM, 
                        I_REGIST_NUM, 
                        I_REMIND_NUM, 
                        I_OFFER_NUM, 
                        I_COLLECT_TIME
                        )
                        VALUES
                        ('%s','%s','%s','%s','%s','%s','%s','%s');

            ''' % (session_id, i_id, i_name, i_access, i_accessensure, i_remind, i_bid, crawl_time)
    try:
        cur.execute(sql)
        conn.commit()
        print('数据成功插入')
    except Exception as e:
        print(e)
        print('数据插入异常')
    cur.close()
    conn.close()


# 拍品
def get_item(session_id):
    url = 'https://paimai.jd.com/album/{}'.format(session_id)
    # url = 'https://paimai.jd.com/album/766495'
    responses = request_200(url)
    if responses == 0:
        print('拍品获取出错')
        return ''
    datas = etree.HTML(responses.text)
    hot_lists = datas.xpath('//*[@id="hotList_ul"]/li')
    pro_lists = datas.xpath('//*[@id="proList_ul"]/li')
    for hot_list in hot_lists:
        i_id = hot_list.xpath('./@k')[0] if hot_list.xpath('./@k') else ''
        i_name = hot_list.xpath('./a/@title')[0] if hot_list.xpath('./@k') else ''
        if i_id:
            i_access, i_accessensure, i_bid = get_item_num(i_id)
            # 获取提醒人数
            # i_remind = get_item_remindCount(i_id)
            i_remind = ''
        else:
            continue
        crawl_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        insert_SQL(session_id, i_id, i_name, i_access, i_accessensure, i_remind, i_bid, crawl_time)
        # print(session_id, i_id, i_name, i_access, i_accessensure, i_remind, i_bid, crawl_time)
        # break
    for pro_list in pro_lists:
        i_id = pro_list.xpath('./@k')[0] if pro_list.xpath('./@k') else ''
        i_name = pro_list.xpath('./a/@title')[0] if pro_list.xpath('./@k') else ''
        if i_id:
            i_access, i_accessensure, i_bid = get_item_num(i_id)
            # 获取提醒人数
            # i_remind = get_item_remindCount(i_id)
            i_remind = ''
        else:
            continue
        crawl_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        insert_SQL(session_id, i_id, i_name, i_access, i_accessensure, i_remind, i_bid, crawl_time)
        # print(session_id, i_id, i_name, i_access, i_accessensure, i_remind, i_bid, crawl_time)
        # break


# 创建Excel
def make_excel(now):
    myexcel = xlwt.Workbook()
    sheet1 = myexcel.add_sheet('专场数据')
    sheet2 = myexcel.add_sheet('拍品数据')
    while True:
        try:
            conn = pymysql.connect(host='10.10.10.77', user='hanruofei', password='hrf123', db='epai_spider_2019_hanruofei',
                                   charset="utf8")
            cur = conn.cursor()
            break
        except Exception as e:
            print(e)
            time.sleep(2)
    sql = '''SELECT * FROM base_collect_send_id;
    '''
    session_end = '0'
    item_end = '0'
    try:
        cur.execute(sql)
        result = cur.fetchall()
        if result:
            end_list = result[-1]
            session_end = end_list[1]
            item_end = end_list[2]
    except Exception as e:
        print(e)
    new_session_end = ''
    new_item_end = ''

    # 创建专场表
    i, j = 0, 0
    session_id_lists = []
    sheet1.write(i, j, '京东专场名称')
    sheet1.write(i, j+1, '京东专场ID')
    sheet1.write(i, j+2, '围观人数')
    sheet1.write(i, j+3, '提醒人数')
    sheet1.write(i, j+4, '出价次数')
    sheet1.write(i, j+5, '截拍时间')
    sheet1.write(i, j+6, '拍品数量')
    sheet1.write(i, j+7, '专场链接')
    sheet1.write(i, j+8, '爬取时间')
    i += 1
    sql = '''SELECT * FROM base_collect_sessions WHERE COLLECT_SESSION_ID > {} ORDER BY COLLECT_SESSION_ID;
    '''.format(session_end)
    #  WHERE S_COLLECT_TIME LIKE '%s-%02d-%02d %02d%s' % (now.year, now.month, now.day, now.hour, '%')
    try:
        cur.execute(sql)
        result1 = cur.fetchall()
        for data1 in result1[::-1]:
            if not new_session_end:
                new_session_end = data1[0]
            if data1[1] not in session_id_lists:
                session_id_lists.append(data1[1])
            else:
                continue
            sheet1.write(i, j, data1[2])
            sheet1.write(i, j+1, data1[1])
            sheet1.write(i, j+2, data1[4])
            sheet1.write(i, j+3, data1[5])
            sheet1.write(i, j+4, data1[6])
            sheet1.write(i, j+5, data1[9])
            sheet1.write(i, j+6, data1[8])
            sheet1.write(i, j+7, 'https://paimai.jd.com/album/' + str(data1[1]))
            sheet1.write(i, j+8, data1[7])
            i += 1
    except Exception as e:
        print(e)

    # 创建拍品表
    i, j = 0, 0
    item_id_lists = []
    sheet2.write(i, j, '拍品名称')
    sheet2.write(i, j+1, '围观人数')
    sheet2.write(i, j+2, '报名人数')
    sheet2.write(i, j+3, '提醒人数')
    sheet2.write(i, j+4, '出价次数')
    # sheet2.write(i ,j+5, '截拍时间')
    sheet2.write(i, j+5, '专场链接')
    sheet2.write(i, j+6, '拍品链接')
    sheet2.write(i, j+7, '爬取时间')
    sheet2.write(i, j+8, '专场id')
    i += 1
    sql = '''SELECT * FROM base_collect_items WHERE COLLECT_ITEM_ID > {} ORDER BY COLLECT_ITEM_ID;
    '''.format(item_end)
    #  WHERE I_COLLECT_TIME LIKE '%s-%02d-%02d %02d%s' % (now.year, now.month, now.day, now.hour, '%')
    try:
        cur.execute(sql)
        result2 = cur.fetchall()
        for data2 in result2[::-1]:
            if not new_item_end:
                new_item_end = data2[0]
            if str(data2[5]) == '0':
                continue
            if data2[2] not in item_id_lists:
                item_id_lists.append(data2[2])
            else:
                continue
            sheet2.write(i, j, data2[3])
            sheet2.write(i, j+1, data2[4])
            sheet2.write(i, j+2, data2[5])
            sheet2.write(i, j+3, data2[6])
            sheet2.write(i, j+4, data2[7])
            sheet2.write(i, j+5, 'https://paimai.jd.com/album/' + str(data2[1]))
            sheet2.write(i, j+6, 'https://paimai.jd.com/' + str(data2[2]))
            sheet2.write(i, j+7, data2[8])
            sheet2.write(i, j+8, data2[1])
            i += 1
    except Exception as e:
        print(e)
    now = datetime.datetime.now()
    excel_name = 'JD_Datas_%s-%s %s.xls' % (now.month, now.day, now.hour)
    save_excel_name = 'D:\project\excel\JD_Datas_%s-%s %s.xls' % (now.month, now.day, now.hour)
    myexcel.save(save_excel_name)

    # try:
    #     drop_excel_name = 'D:\project\excel\JD_Datas_%s-%s %s.xls' % (now.month, now.day-7, now.hour)
    #     os.remove(drop_excel_name)
    # except Exception as e:
    #     print(e)

    send_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    sql = '''INSERT INTO base_collect_send_id 
                ( 
                session_end_id, 
                item_end_id,
                send_time
                )
                VALUES
                ('%s', '%s', '%s');
    ''' % (new_session_end, new_item_end, send_time)
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print(e)
        print('发送节点插入失败')
    cur.close()
    conn.close()
    return save_excel_name, excel_name


# 发送邮件
def send_email(save_excel_name, excel_name):
    # 第三方 SMTP 服务
    mail_host = "smtp.263.net"
    mail_user = "hanruofei@epailive.com"
    mail_pass = "epai706"

    sender = 'hanruofei@epailive.com'
    # to_receiver = ['yangfurong@epailive.com']
    # cc_receiver = ['xieruixin@epailive.com','heshuang@epailive.com','mabinbin@epailive.com']
    #
    # 测试
    # to_receiver = ['hanruofei@epailive.com']
    to_receiver = ['hanruofei@epailive.com', 'heshuang@epailive.com', 'mabinbin@epailive.com']
    cc_receiver = ['hanruofei@epailive.com']

    receivers = to_receiver + cc_receiver

    message = MIMEMultipart('related')

    message['From'] = sender
    message['To'] = ";".join(to_receiver)
    message['Cc'] = ";".join(cc_receiver)

    subject = '京东专场/拍品数据'
    message['Subject'] = Header(subject, 'utf-8')

    content_type = mimetypes.guess_type(save_excel_name)
    # 构造附件1，传送当前目录下的 test.txt 文件
    att1 = MIMEText(open(save_excel_name, 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = content_type[0]

    # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    # 中文名称
    # att1.add_header("Content-Disposition", "attachment",filename=("gb2312", "", "已报名、专场围观数据.xlsx"))

    # 英文名称
    att1["Content-Disposition"] = 'attachment; filename=%s' % excel_name
    message.attach(att1)

    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect('smtp.263.net', 25)
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")


def job_1():
    now = datetime.datetime.now()
    print('开始job_1')
    main_url = "https://pro.jd.com/mall/active/43KQGFpu7JT8QYk4jHQJD9rZn1hE/index.html"
    main_html = request_200(main_url)
    if main_html == 0:
        print('job_1错误')
        return ''
    main_html.encoding = 'utf8'
    datas = main_html.text
    session_list = re.findall('"link":"(\d+)"', datas)
    if session_list:
        get_session(session_list)
        # get_item(session_list)
        p = Pool(6)
        for session_id in session_list:
            p.apply_async(get_item, args=(session_id, ))
        p.close()
        p.join()
    print('结束job_1', now.day, now.hour)


def job_2():
    print('开始job_2')
    now = datetime.datetime.now()
    save_excel_name, excel_name = make_excel(now)
    time.sleep(2)
    send_email(save_excel_name, excel_name)
    print('结束job_2')


if __name__ == '__main__':
    start_time = time.time()
    schedule.every().day.at("14:10").do(job_1)
    schedule.every().day.at("21:45").do(job_1)
    # schedule.every().day.at("13:52").do(job_2)
    schedule.every().thursday.at("14:17").do(job_2)
    # schedule.every().friday.at("09:55").do(job_2)
    # schedule.every().day.at("21:52").do(job_2)
    # job_1()
    while True:
        schedule.run_pending()
        time.sleep(10)

    # job_1()


    # print(time.time() - start_time)

'https://paimai.jd.com/json/current/queryCurAlbumInfo?albumIdStr=725304'
'''
[
    {
        "summary":"英国",
        "albumName":"古家珍品绘画收藏专场",
        "albumId":"725304",
        "auctionStatus":1,
        "displayStatus":1,
        "proCount":16,
        "access":"421",
        "endTime":1565848800000,
        "vendorIdName":"易拍全球",
        "vendorId":704331,
        "startTime":1564711200000,
        "dealCount":"0",
        "templateType":0,
        "remainTime":157763146,
        "mainImagePath":"jfs/t1/40849/12/10606/438943/5d42943dE159a51a9/9533e51649d62527.png",
        "bidCount":"1"
    }
]
'''
'''
https://api.m.jd.com/api?jsonp=jQuery6882087&appid=auctionRemind&functionId=queryRemind&body=%7B%22id%22%3A%22725354%22%2C%22remindType%22%3A2%7D&rand=1565692963887&_=1565692961903
https://api.m.jd.com/api?jsonp=jQuery8429905&appid=auctionRemind&functionId=queryRemind&body=%7B%22id%22%3A%22725354%22%2C%22remindType%22%3A2%7D&rand=1565745475472&_=1565745473399
https://api.m.jd.com/api?jsonp=jQuery357913&appid=auctionRemind&functionId=queryRemind&body=%7B%22id%22%3A%22725355%22%2C%22remindType%22%3A2%7D&rand=1565746643102&_=1565746641108
{"status":200,"message":"已设置提醒","data":{"id":7921854,"paimaiId":0,"specialId":725354,"remindType":2,"pin":"h***********7","phone":"173****2237","isBeginRemind":0,"beginRemindStatus":3,"isEndRemind":1,"endRemindStatus":1,"createTime":1565692761000,"lastModifyTime":1565746845000,"endRemindTime":1565760600000,"remindSource":0,"isStockRemind":0,"stockRemindStatus":3},"remindCount":1,"code":0}
{"status":101,"message":"未发现提醒","data":{"phone":"173****2237"},"remindCount":0,"code":0}
https://paimai.jd.com/services/currentPriceList.action?albumId=742647&callback=updateCurrData&t=1565754665218&callback=jQuery4018460&_=1565754665219
'''
'''
https://api.m.jd.com/api?appid=paimai&functionId=getPaimaiRealTimeData&body={%22end%22:9,%22paimaiId%22:110688909,%22source%22:0,%22start%22:0}&loginType=3
https://api.m.jd.com/api?appid=paimai&functionId=getPaimaiRealTimeData&body={%22end%22:9,%22paimaiId%22:110688909,%22source%22:0,%22start%22:0}&loginType=3
https://api.m.jd.com/api?appid=auctionRemind&functionId=queryRemind&body={%22id%22:110688909,%22remindType%22:1}&loginType=3
https://api.m.jd.com/api?appid=auctionRemind&functionId=queryRemind&body={"id":110688909,"remindType":1}&loginType=3
https://api.m.jd.com/api?appid=auctionRemind&functionId=queryRemind&body={"id":110688909,"remindType":1}&loginType=3
'''
