import requests
import re
import json
import time
import datetime
import redis
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# 创建基类
Base = declarative_base()


# 定义映射对象
class ZhixiaoTable(Base):
    # 表名
    __tablename__ = "small_program"
    # 表结构
    id = Column(Integer, primary_key=True)  # id
    program_style = Column('program_style', String(20))  # 小程序分类
    program_icon = Column('program_icon', String(20))  # 小程序图标
    program_title = Column('program_title', String(20))  # 小程序主标题
    program_user_name = Column('program_user_name', String(20))  # 开发者名称
    program_see_num = Column('program_see_num', Integer)  # 浏览量
    program_category_id = Column('program_category_id', String(20))  # 分类id
    release_time = Column('release_time', DateTime)  # 发布时间
    update_time = Column('update_time', DateTime)  # 修改时间
    create_time = Column('create_time', DateTime)  # 创建时间
    program_qrcode = Column('program_qrcode', String(20))  # 二维码
    program_pic = Column('program_pic', Text)  # 图片展示
    program_desc = Column('program_desc', Text)  # 描述
    program_score_id = Column('program_score_id', String(20))  # 评分id
    program_comment_id = Column('program_comment_id', String(20))  # 评论id
    program_fabulous_id = Column('program_fabulous_id', String(20))  # 点赞id
    program_tab_id = Column('program_tab_id', String(20))  # 标签id
    program_audit_status = Column('program_audit_status', String(20))  # 审核状态


# 初始化数据库连接
engine = create_engine('mysql://root:@localhost:3306/luobo?charset=utf8mb4', echo=False)

# 创建DBSession
DBSession = sessionmaker(bind=engine)

# 创建session ORM映射对象
session = DBSession()

# redis链接
r = redis.Redis(host='127.0.0.1', port=6379, db=0)


# 关闭数据库连接
def closeDB():
    session.close()


# 抓取
def crawl(targetId):
    rsp = requests.get('https://minapp.com/api/v5/trochili/miniapp/' +
                       str(targetId) + '/')
    if rsp.status_code != 200:
        print("get " + str(targetId) + " error:" + str(rsp.status_code))
        return
    saveDB(rsp.content)


# 存储
def saveDB(jsonString):

    object = json.loads(jsonString)


    tags = []
    for tag in object['tag']:
        tags.append(tag['name'])
    screenshots = []
    for screenshot in object['screenshot']:
        screenshots.append(screenshot['image'])

    classify = ','.join(tags)
    screenshots = ','.join(screenshots)

    zx_obj = ZhixiaoTable(
        program_style="1",
        program_icon=object['icon']['image'],
        program_title=object['name'],
        program_user_name=object['created_by'],
        program_see_num=object['visit_amount'],
        program_category_id=classify,
        release_time=datetime.datetime.fromtimestamp(object['created_at']),
        update_time=datetime.datetime.fromtimestamp(object['created_at']),
        create_time=datetime.datetime.fromtimestamp(object['created_at']),
        program_qrcode=object['qrcode']['image'],
        program_pic=screenshots,
        program_desc=object['description'],
        program_score_id=r.incr("program_score_id"),
        program_comment_id=r.incr("program_comment_id"),
        program_fabulous_id=r.incr("program_fabulous_id"),
        program_audit_status="2",
        program_tab_id=r.incr("program_tab_id"))

    print(zx_obj)
    session.add(zx_obj)
    session.commit()
    print("save " + str(object['id']) + " success name:" + object['name'])


def main():
    f = open('./error_index.txt', 'a+')
    for index in range(6002, 6003):
        try:
            crawl(index)
        except:
            f.write(str(index) + '\n')
            print('handler error with ' + str(index))
        else:
            time.sleep(1)
    print("zhixiao data all over!")

    closeDB()


if __name__ == '__main__':
    main()
