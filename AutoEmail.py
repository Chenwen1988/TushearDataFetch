# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 18:25:27 2018

@author: Administrator
"""

import os
import json
import smtplib
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

__PATH_FILE = os.path.dirname(__file__)


def send_email(subject, path, receiver, content):
    email_host = 'smtp.bbdservice.com'  # 服务器地址
    with open(path) as file:
        credentials_json = json.loads(file.read())
    sender = credentials_json.get('acc')    # 发件人
    password = credentials_json.get('pwd')  # 密码，如果是授权码就填授权码

    msg = MIMEMultipart()
    msg['Subject'] = subject        # 标题
    msg['From'] = 'chen.chen'       # 发件人昵称
    msg['To'] = ','.join(receiver)  # 收件人昵称
    # msg['To'] = receiver            # 收件人昵称

    # string = 'RetrieveTushareData.py is down! \n'
    # string += content
    # msg.attach(MIMEText(string))

    msg.attach(MIMEText(content))

    # # 附件-图片
    # image = MIMEImage(open(r'MinShengHanWu1.png', 'rb').read())
    # image.add_header('Content-Disposition', 'attachment', filename='MinShengHanWu1.png')
    # msg.attach(image)
    
    # # 附件-文件
    # file_html =glob.glob(path+'*.html')
    # for file in file_html:
    #     print(file)
    #     att = MIMEText(open(file, 'rb').read(), 'base64', 'utf-8')
    #     att["Content-Type"] = 'application/octet-stream'
    #     att["Content-Disposition"] = 'attachment; filename="%s"' % file
    #     msg.attach(att)

    # 发送
    smtp = smtplib.SMTP()
    smtp.connect(email_host, 25)
    smtp.login(sender, password)
    smtp.sendmail(sender, receiver, msg.as_string())
    smtp.quit()


if __name__ == '__main__':
    receivers = ['chenchen@bbdservice.com']  # 收件人
    sub = '自动邮件：崩@TushareData'
    sender_info = os.path.join('Config', 'credential.json')
    send_email(sub, sender_info, receivers, 'gg了')
