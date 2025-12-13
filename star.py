import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings
import sqlite3
import os
import threading
import matplotlib.pyplot as plt
import matplotlib
import time

matplotlib.use('Agg')
warnings.filterwarnings('ignore')

# ==================== 页面配置 ====================
st.set_page_config(
    page_title="演唱会管理信息系统",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ==================== 数据库初始化 ====================
def ensure_database_initialized():
    """确保数据库已初始化"""
    db_path = "concert_management.db"

    # 如果数据库不存在，或者存在但为空（小于1KB），则重新初始化
    if not os.path.exists(db_path) or os.path.getsize(db_path) < 1024:
        st.info("🔧 首次运行或数据库异常，正在初始化数据库...")

        # 使用简单的进度指示器
        status_placeholder = st.empty()

        # 创建数据库文件
        status_placeholder.text("创建数据库文件...")
        try:
            # 如果数据库文件存在但损坏，先删除
            if os.path.exists(db_path):
                os.remove(db_path)

            # 连接到数据库（会自动创建文件）
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")

            # 创建表结构
            status_placeholder.text("创建表结构...")

            # 创建歌手表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS singers (
                    singer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    birth_date DATE,
                    nationality TEXT,
                    debut_year INTEGER,
                    genre TEXT,
                    active_status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # 创建演唱会表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS concerts (
                    concert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    singer_id INTEGER,
                    concert_name TEXT NOT NULL,
                    concert_date DATE,
                    city TEXT,
                    venue TEXT,
                    capacity INTEGER,
                    attendance INTEGER,
                    ticket_price REAL,
                    revenue REAL,
                    attendance_rate REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (singer_id) REFERENCES singers(singer_id)
                )
            ''')

            # 创建热度表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS popularity (
                    popularity_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    singer_id INTEGER,
                    record_date DATE,
                    fan_count INTEGER,
                    topic_score REAL,
                    popularity_score REAL,
                    social_media_mentions INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (singer_id) REFERENCES singers(singer_id)
                )
            ''')

            # 创建城市表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS cities (
                    city_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city_name TEXT NOT NULL,
                    country TEXT,
                    population INTEGER,
                    avg_concert_capacity INTEGER,
                    concert_frequency INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            conn.commit()
            status_placeholder.text("表结构创建完成，正在插入示例数据...")

            # 插入真实歌手数据
            real_singers = [
                ('周杰伦', '1979-01-18', '中国', 2000, '流行/R&B', '活跃'),
                ('林俊杰', '1981-03-27', '新加坡', 2003, '流行', '活跃'),
                ('邓紫棋', '1991-08-16', '中国', 2008, '流行', '活跃'),
                ('五月天', '1997-03-29', '中国', 1999, '摇滚', '活跃'),
                ('Taylor Swift', '1989-12-13', '美国', 2006, '流行/乡村', '活跃'),
                ('陈奕迅', '1974-07-27', '中国', 1995, '流行', '活跃'),
                ('张学友', '1961-07-10', '中国', 1984, '流行', '活跃'),
                ('王菲', '1969-08-08', '中国', 1989, '流行', '活跃'),
                ('李荣浩', '1985-07-11', '中国', 2013, '流行', '活跃'),
                ('薛之谦', '1983-07-17', '中国', 2005, '流行', '活跃'),
                ('蔡徐坤', '1998-08-02', '中国', 2018, '流行', '活跃'),
                ('张杰', '1982-12-20', '中国', 2004, '流行', '活跃'),
                ('华晨宇', '1990-02-07', '中国', 2013, '流行/摇滚', '活跃'),
                ('毛不易', '1994-10-01', '中国', 2017, '流行', '活跃'),
                ('刘德华', '1961-09-27', '中国', 1981, '流行', '活跃'),
                ('王力宏', '1976-05-17', '美国', 1995, '流行/R&B', '活跃'),
                ('张惠妹', '1972-08-09', '中国', 1996, '流行', '活跃'),
                ('孙燕姿', '1978-07-23', '新加坡', 2000, '流行', '活跃'),
                ('蔡依林', '1980-09-15', '中国', 1999, '流行', '活跃'),
                ('刘若英', '1970-06-01', '中国', 1991, '流行', '活跃')
            ]

            cursor = conn.cursor()
            for singer in real_singers:
                cursor.execute('''
                    INSERT INTO singers (name, birth_date, nationality, debut_year, genre, active_status)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', singer)

            # 插入城市数据
            real_cities = [
                ('北京', '中国', 2189, 45000, 15),
                ('上海', '中国', 2487, 38000, 18),
                ('广州', '中国', 1867, 35000, 12),
                ('深圳', '中国', 1756, 30000, 10),
                ('成都', '中国', 2093, 40000, 8),
                ('杭州', '中国', 1193, 28000, 7),
                ('南京', '中国', 942, 25000, 6),
                ('武汉', '中国', 1245, 32000, 9),
                ('西安', '中国', 1295, 28000, 5),
                ('重庆', '中国', 3205, 35000, 7),
                ('香港', '中国', 750, 20000, 8),
                ('台北', '中国', 275, 15000, 6),
                ('新加坡', '新加坡', 545, 25000, 5),
                ('吉隆坡', '马来西亚', 180, 20000, 4),
                ('东京', '日本', 1393, 30000, 10),
                ('首尔', '韩国', 977, 25000, 8),
                ('曼谷', '泰国', 1050, 22000, 6),
                ('伦敦', '英国', 898, 35000, 12),
                ('纽约', '美国', 839, 40000, 15),
                ('洛杉矶', '美国', 397, 30000, 10)
            ]

            for city in real_cities:
                cursor.execute('''
                    INSERT INTO cities (city_name, country, population, avg_concert_capacity, concert_frequency)
                    VALUES (?, ?, ?, ?, ?)
                ''', city)

            status_placeholder.text("数据插入完成，正在生成演唱会记录...")

            # 生成示例演唱会数据
            # for singer_id in range(1, 6):  # 为前5位歌手生成演唱会数据
            #     for i in range(3):  # 每位歌手3场演唱会
            #         year = 2024
            #         month = np.random.randint(1, 13)
            #         day = np.random.randint(1, 28)
            #         city = np.random.choice(['北京', '上海', '广州', '深圳'])
            #         capacity = np.random.choice([10000, 15000, 20000])
            #         attendance = int(capacity * np.random.uniform(0.8, 0.95))
            #         ticket_price = np.random.choice([300, 500, 800])
            #         revenue = attendance * ticket_price
            #         attendance_rate = attendance / capacity
            #
            #         cursor.execute('''
            #             INSERT INTO concerts
            #             (singer_id, concert_name, concert_date, city, venue, capacity,
            #              attendance, ticket_price, revenue, attendance_rate)
            #             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            #         ''', (
            #             singer_id,
            #             f'2024巡回演唱会-{city}站',
            #             f'{year}-{month:02d}-{day:02d}',
            #             city,
            #             '大型体育场',
            #             capacity,
            #             attendance,
            #             ticket_price,
            #             revenue,
            #             attendance_rate
            #         ))

            # ... 前面的代码保持不变 ...

            status_placeholder.text("数据插入完成，正在生成演唱会记录...")

            # 修改这里：为更多歌手生成演唱会数据，并确保收入不为0
            try:
                # 获取所有歌手ID
                cursor.execute("SELECT singer_id FROM singers")
                all_singers = [row[0] for row in cursor.fetchall()]

                concerts_added = 0
                # 为每位歌手生成2-5场演唱会
                for singer_id in all_singers[:10]:  # 至少为前10位歌手生成数据
                    num_concerts = np.random.randint(2, 6)

                    for i in range(num_concerts):
                        year = 2023 + np.random.randint(0, 2)  # 2023或2024年
                        month = np.random.randint(1, 13)
                        day = np.random.randint(1, 28)

                        cities = ['北京', '上海', '广州', '深圳', '成都', '杭州', '南京', '武汉', '西安', '重庆']
                        city = np.random.choice(cities)

                        # 不同的场馆容量和票价
                        capacity_options = [
                            (5000, 300, '小型体育馆'),
                            (10000, 500, '中型体育馆'),
                            (20000, 800, '大型体育馆'),
                            (50000, 1000, '体育场'),
                            (80000, 1200, '大型体育场')
                        ]
                        capacity, base_price, venue = np.random.choice(capacity_options, p=[0.1, 0.3, 0.4, 0.15, 0.05])

                        # 上座率在70%-100%之间
                        attendance_rate = np.random.uniform(0.7, 1.0)
                        attendance = int(capacity * attendance_rate)

                        # 票价波动
                        ticket_price_variation = np.random.uniform(0.8, 1.2)
                        ticket_price = int(base_price * ticket_price_variation)

                        # 计算收入
                        revenue = attendance * ticket_price

                        # 演唱会名称
                        concert_name = f"{year}年巡回演唱会-{city}站"

                        cursor.execute('''
                            INSERT INTO concerts 
                            (singer_id, concert_name, concert_date, city, venue, capacity, 
                             attendance, ticket_price, revenue, attendance_rate)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            singer_id,
                            concert_name,
                            f'{year}-{month:02d}-{day:02d}',
                            city,
                            venue,
                            capacity,
                            attendance,
                            ticket_price,
                            revenue,
                            attendance_rate
                        ))

                        concerts_added += 1

                conn.commit()
                status_placeholder.text(f"✅ 已生成 {concerts_added} 场演唱会数据")

            except Exception as e:
                status_placeholder.text(f"生成演唱会数据时出错: {str(e)}")
                # 使用简单的示例数据作为备份
                cursor.execute('''
                    INSERT INTO concerts 
                    (singer_id, concert_name, concert_date, city, venue, capacity, 
                     attendance, ticket_price, revenue, attendance_rate)
                    VALUES 
                    (1, '2024世界巡回演唱会-北京站', '2024-05-01', '北京', '国家体育场', 80000, 75000, 1000, 75000000, 0.9375),
                    (1, '2024世界巡回演唱会-上海站', '2024-06-15', '上海', '上海体育场', 50000, 48000, 800, 38400000, 0.96),
                    (2, 'JJ20世界巡回演唱会-北京站', '2024-04-20', '北京', '凯迪拉克中心', 18000, 17000, 950, 16150000, 0.944),
                    (2, 'JJ20世界巡回演唱会-广州站', '2024-07-10', '广州', '广州体育馆', 12000, 11800, 850, 10030000, 0.983),
                    (3, 'Queen of Hearts世界巡演-上海站', '2024-08-20', '上海', '虹口足球场', 35000, 33000, 600, 19800000, 0.943)
                ''')
                conn.commit()
                status_placeholder.text("✅ 已插入示例演唱会数据")


            status_placeholder.text("正在生成热度数据...")

            # 生成热度数据
            for singer_id in range(1, 6):
                base_fans = np.random.randint(500000, 2000000)
                for month in range(1, 13):
                    record_date = f'2023-{month:02d}-01'
                    fan_count = int(base_fans * (1 + month * 0.05))
                    topic_score = np.random.uniform(60, 95)
                    popularity_score = np.random.uniform(60, 95)
                    social_media = np.random.randint(10000, 50000)

                    cursor.execute('''
                        INSERT INTO popularity 
                        (singer_id, record_date, fan_count, topic_score, popularity_score, social_media_mentions)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (singer_id, record_date, fan_count, topic_score, popularity_score, social_media))

            conn.commit()
            cursor.close()
            conn.close()

            status_placeholder.text("✅ 数据库初始化完成！")
            st.success("数据库已成功初始化，正在刷新页面...")
            time.sleep(2)
            st.rerun()

        except Exception as e:
            status_placeholder.text(f"❌ 数据库初始化失败：{str(e)}")
            st.error(f"数据库初始化失败：{str(e)}")
            # 显示简单数据库创建选项
            if st.button("尝试简单初始化"):
                try:
                    # 创建最简单的数据库文件
                    conn = sqlite3.connect(db_path, check_same_thread=False)
                    conn.execute('CREATE TABLE IF NOT EXISTS singers (id INTEGER PRIMARY KEY, name TEXT)')
                    conn.execute('CREATE TABLE IF NOT EXISTS concerts (id INTEGER PRIMARY KEY, name TEXT)')
                    conn.execute('INSERT INTO singers (name) VALUES ("示例歌手")')
                    conn.commit()
                    conn.close()
                    st.success("已创建简单数据库，请刷新页面")
                    time.sleep(2)
                    st.rerun()
                except Exception as e2:
                    st.error(f"简单初始化也失败：{str(e2)}")
            return False
    return True


# 在应用开始时检查数据库
if not ensure_database_initialized():
    st.stop()



# ==================== 线程本地存储 ====================
_thread_local = threading.local()


def check_and_initialize_database():
    """检查并初始化数据库"""
    if not os.path.exists("concert_management.db"):
        st.info("🔧 首次运行，正在初始化数据库...")

        # 显示进度条
        progress_bar = st.progress(0)
        status_text = st.empty()

        # 步骤1：创建数据库文件
        status_text.text("创建数据库文件...")
        open("concert_management.db", 'w').close()
        progress_bar.progress(25)

        # 步骤2：创建表结构
        status_text.text("创建表结构...")
        conn = sqlite3.connect("concert_management.db", check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # 创建所有表
        init_database()
        progress_bar.progress(50)

        # 步骤3：插入示例数据
        status_text.text("插入示例数据...")
        insert_real_data()
        progress_bar.progress(75)

        # 步骤4：完成
        status_text.text("完成初始化...")
        conn.close()
        progress_bar.progress(100)
        status_text.text("✅ 数据库初始化完成！")

        # 等待并重新加载
        time.sleep(2)
        return True
    return False


# 在主程序开始前调用
if __name__ == "__main__":
    # 检查是否需要初始化
    if check_and_initialize_database():
        # 重新加载页面
        st.rerun()

def init_database():
    """初始化数据库表"""
    conn = get_db_connection()
    if conn:
        try:
            # 创建歌手表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS singers (
                    singer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    birth_date DATE,
                    nationality TEXT,
                    debut_year INTEGER,
                    genre TEXT,
                    active_status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # 创建演唱会表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS concerts (
                    concert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    singer_id INTEGER,
                    concert_name TEXT NOT NULL,
                    concert_date DATE,
                    city TEXT,
                    venue TEXT,
                    capacity INTEGER,
                    attendance INTEGER,
                    ticket_price REAL,
                    revenue REAL,
                    attendance_rate REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (singer_id) REFERENCES singers(singer_id)
                )
            ''')

            # 创建热度表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS popularity (
                    popularity_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    singer_id INTEGER,
                    record_date DATE,
                    fan_count INTEGER,
                    topic_score REAL,
                    popularity_score REAL,
                    social_media_mentions INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (singer_id) REFERENCES singers(singer_id)
                )
            ''')

            # 创建城市表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS cities (
                    city_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city_name TEXT NOT NULL,
                    country TEXT,
                    population INTEGER,
                    avg_concert_capacity INTEGER,
                    concert_frequency INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            conn.commit()
            print("数据库表初始化完成")
        except Exception as e:
            print(f"数据库初始化失败: {str(e)}")


def get_db_connection():
    """获取当前线程的数据库连接"""
    if not hasattr(_thread_local, "conn") or _thread_local.conn is None:
        try:
            # SQLite数据库文件路径
            db_path = "concert_management.db"

            # 创建数据库连接，启用check_same_thread=False以支持多线程
            conn = sqlite3.connect(db_path, check_same_thread=False)

            conn.text_factory = str

            # 设置返回字典格式
            conn.row_factory = sqlite3.Row

            # 启用外键支持
            conn.execute("PRAGMA foreign_keys = ON")

            # 设置文本编码为UTF-8
            conn.execute("PRAGMA encoding = 'UTF-8'")

            _thread_local.conn = conn
            print(f"线程 {threading.current_thread().name} 创建了新的数据库连接")

        except Exception as e:
            print(f"创建数据库连接失败: {str(e)}")
            _thread_local.conn = None

    return _thread_local.conn


def close_db_connection():
    """关闭当前线程的数据库连接"""
    if hasattr(_thread_local, "conn") and _thread_local.conn:
        try:
            _thread_local.conn.close()
            print(f"线程 {threading.current_thread().name} 关闭了数据库连接")
        except:
            pass
        finally:
            _thread_local.conn = None


# ==================== 数据库查询函数 ====================
@st.cache_data(ttl=600)
def query_database(query, params=None):
    """执行数据库查询"""
    conn = get_db_connection()

    if conn is None:
        print("没有可用的数据库连接")
        return None

    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # 获取列名
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            data = cursor.fetchall()
            # 转换为字典列表
            data_dicts = []
            for row in data:
                row_dict = {}
                for col_name, value in zip(columns, row):
                    # 如果值是字节类型，转换为字符串
                    if isinstance(value, bytes):
                        try:
                            value = value.decode('utf-8')
                        except:
                            # 如果解码失败，使用错误处理
                            try:
                                value = value.decode('utf-8', errors='ignore')
                            except:
                                value = str(value)
                    # 如果值是None，转换为空字符串
                    elif value is None:
                        value = ''
                    row_dict[col_name] = value
                data_dicts.append(row_dict)

            df = pd.DataFrame(data_dicts)
            # 确保列名都是小写
            if not df.empty:
                df.columns = [col.lower() for col in df.columns]
                # 确保特定数值列是数值类型
                numeric_columns = ['singer_id', 'concert_id', 'popularity_id', 'city_id',
                                   'capacity', 'attendance', 'ticket_price', 'revenue',
                                   'attendance_rate', 'fan_count', 'topic_score',
                                   'popularity_score', 'social_media_mentions',
                                   'population', 'avg_concert_capacity', 'concert_frequency',
                                   'debut_year']

                for col in numeric_columns:
                    if col in df.columns:
                        # 先转换为字符串，再转换为数值
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # 确保日期列是日期类型
                date_columns = ['birth_date', 'concert_date', 'record_date', 'created_at']
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')

        else:
            df = pd.DataFrame()

        cursor.close()
        return df

    except Exception as e:
        print(f"数据库查询失败: {str(e)}")
        return None


# ==================== 数据获取函数 ====================
@st.cache_data(ttl=600)
def get_data(table_name):
    """从数据库获取数据"""
    if table_name == 'singers':
        query = "SELECT * FROM singers ORDER BY singer_id"
    elif table_name == 'concerts':
        query = "SELECT * FROM concerts ORDER BY concert_date DESC"
    elif table_name == 'popularity':
        query = "SELECT * FROM popularity ORDER BY record_date DESC"
    elif table_name == 'cities':
        query = "SELECT * FROM cities ORDER BY city_id"
    else:
        query = f"SELECT * FROM {table_name}"

    df = query_database(query)

    if df is None:
        # 如果查询失败，返回空DataFrame
        df = pd.DataFrame()

    return df


# ==================== 数据库操作函数 ====================
def execute_sql(sql, params=None):
    """执行SQL语句（用于INSERT、UPDATE、DELETE）"""
    conn = get_db_connection()

    if conn is None:
        print("没有可用的数据库连接")
        return False

    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        conn.commit()
        cursor.close()
        print(f"SQL执行成功: {sql[:50]}...")
        return True
    except Exception as e:
        print(f"执行SQL失败: {str(e)}")
        try:
            conn.rollback()
        except:
            pass
        return False


# ==================== 初始化数据库并插入真实数据 ====================
def initialize_database():
    """初始化数据库并插入真实数据"""
    # 首先创建数据库文件（如果不存在）
    db_path = "concert_management.db"
    if not os.path.exists(db_path):
        # 创建一个空文件
        open(db_path, 'w').close()

    # 创建数据库连接
    conn = get_db_connection()
    if conn is None:
        print("无法创建数据库连接")
        return False

    try:
        # 创建所有表
        init_database()

        # 插入真实数据
        insert_real_data()

        print("数据库初始化完成，真实数据已插入")
        return True

    except Exception as e:
        print(f"数据库初始化失败: {str(e)}")
        return False


def insert_real_data():
    """插入真实的歌手和演唱会数据"""
    conn = get_db_connection()
    if conn is None:
        return

    try:
        cursor = conn.cursor()

        # 检查歌手表是否已有数据
        cursor.execute("SELECT COUNT(*) as count FROM singers")
        singer_count = cursor.fetchone()['count']

        if singer_count == 0:
            # 真实的歌手数据
            real_singers = [
                ('周杰伦', '1979-01-18', '中国', 2000, '流行/R&B', '活跃'),
                ('林俊杰', '1981-03-27', '新加坡', 2003, '流行', '活跃'),
                ('邓紫棋', '1991-08-16', '中国', 2008, '流行', '活跃'),
                ('五月天', None, '中国', 1999, '摇滚', '活跃'),
                ('Taylor Swift', '1989-12-13', '美国', 2006, '流行/乡村', '活跃'),
                ('陈奕迅', '1974-07-27', '中国', 1995, '流行', '活跃'),
                ('张学友', '1961-07-10', '中国', 1984, '流行', '活跃'),
                ('王菲', '1969-08-08', '中国', 1989, '流行', '活跃'),
                ('李荣浩', '1985-07-11', '中国', 2013, '流行', '活跃'),
                ('薛之谦', '1983-07-17', '中国', 2005, '流行', '活跃'),
                ('蔡徐坤', '1998-08-02', '中国', 2018, '流行', '活跃'),
                ('张杰', '1982-12-20', '中国', 2004, '流行', '活跃'),
                ('华晨宇', '1990-02-07', '中国', 2013, '流行/摇滚', '活跃'),
                ('毛不易', '1994-10-01', '中国', 2017, '流行', '活跃'),
                ('刘德华', '1961-09-27', '中国', 1981, '流行', '活跃'),
                ('王力宏', '1976-05-17', '美国', 1995, '流行/R&B', '活跃'),
                ('张惠妹', '1972-08-09', '中国', 1996, '流行', '活跃'),
                ('孙燕姿', '1978-07-23', '新加坡', 2000, '流行', '活跃'),
                ('蔡依林', '1980-09-15', '中国', 1999, '流行', '活跃'),
                ('刘若英', '1970-06-01', '中国', 1991, '流行', '活跃')
            ]

            for singer in real_singers:
                cursor.execute('''
                    INSERT INTO singers (name, birth_date, nationality, debut_year, genre, active_status)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', singer)

            print(f"已插入 {len(real_singers)} 位歌手数据")

        # 检查演唱会表是否已有数据
        cursor.execute("SELECT COUNT(*) as count FROM concerts")
        concert_count = cursor.fetchone()['count']

        if concert_count == 0:
            # 真实的演唱会数据（周杰伦的演唱会）
            jay_concerts = [
                (1, '嘉年华世界巡回演唱会-上海站', '2023-10-01', '上海', '上海体育场', 50000, 48000, 800, 38400000,
                 0.96),
                (1, '嘉年华世界巡回演唱会-北京站', '2023-11-15', '北京', '国家体育场', 80000, 75000, 1000, 75000000,
                 0.94),
                (1, '嘉年华世界巡回演唱会-广州站', '2023-12-10', '广州', '天河体育场', 40000, 39000, 700, 27300000,
                 0.975),
                (1, '嘉年华世界巡回演唱会-深圳站', '2024-01-20', '深圳', '深圳湾体育中心', 35000, 34000, 750, 25500000,
                 0.971),
                (1, '嘉年华世界巡回演唱会-成都站', '2024-03-05', '成都', '成都体育中心', 45000, 42000, 650, 27300000,
                 0.933),
            ]

            # 林俊杰的演唱会
            jj_concerts = [
                (2, 'JJ20世界巡回演唱会-上海站', '2023-09-15', '上海', '梅赛德斯奔驰文化中心', 18000, 17500, 900,
                 15750000, 0.972),
                (2, 'JJ20世界巡回演唱会-北京站', '2023-10-20', '北京', '凯迪拉克中心', 18000, 17000, 950, 16150000,
                 0.944),
                (2, 'JJ20世界巡回演唱会-广州站', '2023-11-25', '广州', '广州体育馆', 12000, 11800, 850, 10030000,
                 0.983),
                (2, 'JJ20世界巡回演唱会-香港站', '2024-01-10', '香港', '红磡体育馆', 12500, 12000, 1000, 12000000,
                 0.96),
            ]

            # 邓紫棋的演唱会
            gemi_concerts = [
                (3, 'Queen of Hearts世界巡演-上海站', '2023-08-20', '上海', '虹口足球场', 35000, 33000, 600, 19800000,
                 0.943),
                (3, 'Queen of Hearts世界巡演-北京站', '2023-09-10', '北京', '工人体育场', 50000, 45000, 550, 24750000,
                 0.9),
                (3, 'Queen of Hearts世界巡演-深圳站', '2023-10-05', '深圳', '深圳体育场', 30000, 29000, 500, 14500000,
                 0.967),
            ]

            # Taylor Swift的演唱会
            taylor_concerts = [
                (5, 'The Eras Tour-东京站', '2024-02-10', '东京', '东京巨蛋', 55000, 53000, 1200, 63600000, 0.964),
                (5, 'The Eras Tour-新加坡站', '2024-03-15', '新加坡', '新加坡国家体育场', 60000, 58000, 1100, 63800000,
                 0.967),
            ]

            all_concerts = jay_concerts + jj_concerts + gemi_concerts + taylor_concerts

            # 为其他歌手生成演唱会数据
            for singer_id in range(4, 21):
                if singer_id == 5:  # Taylor Swift已经添加了
                    continue

                num_concerts = np.random.randint(2, 6)
                for i in range(num_concerts):
                    year = np.random.choice([2022, 2023, 2024])
                    month = np.random.randint(1, 13)
                    day = np.random.randint(1, 28)
                    cities = ['北京', '上海', '广州', '深圳', '成都', '杭州', '南京', '武汉', '西安', '重庆']
                    city = np.random.choice(cities)

                    venues = {
                        '北京': ['国家体育场', '工人体育场', '凯迪拉克中心', '五棵松体育馆'],
                        '上海': ['上海体育场', '梅赛德斯奔驰文化中心', '虹口足球场'],
                        '广州': ['天河体育场', '广州体育馆'],
                        '深圳': ['深圳湾体育中心', '深圳体育场'],
                        '成都': ['成都体育中心'],
                        '杭州': ['黄龙体育中心'],
                        '南京': ['南京奥体中心'],
                        '武汉': ['武汉体育中心'],
                        '西安': ['陕西省体育场'],
                        '重庆': ['重庆奥林匹克体育中心']
                    }

                    venue = np.random.choice(venues[city] if city in venues else ['体育场'])
                    capacity = np.random.choice([10000, 15000, 20000, 30000, 50000, 80000])
                    attendance_rate = np.random.uniform(0.85, 0.99)
                    attendance = int(capacity * attendance_rate)
                    ticket_price = np.random.choice([300, 400, 500, 600, 800, 1000])
                    revenue = attendance * ticket_price

                    all_concerts.append((
                        singer_id,
                        f'{year}巡回演唱会-{city}站',
                        f'{year}-{month:02d}-{day:02d}',
                        city,
                        venue,
                        capacity,
                        attendance,
                        ticket_price,
                        revenue,
                        attendance_rate
                    ))

            for concert in all_concerts:
                cursor.execute('''
                    INSERT INTO concerts 
                    (singer_id, concert_name, concert_date, city, venue, capacity, 
                     attendance, ticket_price, revenue, attendance_rate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', concert)

            print(f"已插入 {len(all_concerts)} 场演唱会数据")

        # 检查城市表是否已有数据
        cursor.execute("SELECT COUNT(*) as count FROM cities")
        city_count = cursor.fetchone()['count']

        if city_count == 0:
            # 真实的城市数据
            real_cities = [
                ('北京', '中国', 2189, 45000, 15),
                ('上海', '中国', 2487, 38000, 18),
                ('广州', '中国', 1867, 35000, 12),
                ('深圳', '中国', 1756, 30000, 10),
                ('成都', '中国', 2093, 40000, 8),
                ('杭州', '中国', 1193, 28000, 7),
                ('南京', '中国', 942, 25000, 6),
                ('武汉', '中国', 1245, 32000, 9),
                ('西安', '中国', 1295, 28000, 5),
                ('重庆', '中国', 3205, 35000, 7),
                ('香港', '中国', 750, 20000, 8),
                ('台北', '中国', 275, 15000, 6),
                ('新加坡', '新加坡', 545, 25000, 5),
                ('吉隆坡', '马来西亚', 180, 20000, 4),
                ('东京', '日本', 1393, 30000, 10),
                ('首尔', '韩国', 977, 25000, 8),
                ('曼谷', '泰国', 1050, 22000, 6),
                ('伦敦', '英国', 898, 35000, 12),
                ('纽约', '美国', 839, 40000, 15),
                ('洛杉矶', '美国', 397, 30000, 10)
            ]

            for city in real_cities:
                cursor.execute('''
                    INSERT INTO cities (city_name, country, population, avg_concert_capacity, concert_frequency)
                    VALUES (?, ?, ?, ?, ?)
                ''', city)

            print(f"已插入 {len(real_cities)} 个城市数据")

        # 检查热度表是否已有数据
        cursor.execute("SELECT COUNT(*) as count FROM popularity")
        pop_count = cursor.fetchone()['count']

        if pop_count == 0:
            # 为每个歌手生成12个月的热度数据
            for singer_id in range(1, 21):
                base_fans = np.random.randint(500000, 10000000)
                for month in range(1, 13):
                    record_date = f'2023-{month:02d}-01'

                    # 模拟粉丝增长
                    month_growth = np.random.uniform(0.98, 1.05)
                    fan_count = int(base_fans * (month_growth ** (month - 1)) * np.random.uniform(0.95, 1.05))

                    # 话题度和传唱度
                    topic_score = np.random.uniform(60, 95)
                    popularity_score = np.random.uniform(60, 95)

                    # 社交媒体提及
                    social_media = np.random.randint(10000, 500000)

                    cursor.execute('''
                        INSERT INTO popularity 
                        (singer_id, record_date, fan_count, topic_score, popularity_score, social_media_mentions)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (singer_id, record_date, fan_count, topic_score, popularity_score, social_media))

            print("已插入热度数据")

        conn.commit()
        cursor.close()

    except Exception as e:
        print(f"插入数据失败: {str(e)}")
        conn.rollback()


# ==================== 页面函数定义 ====================
def show_system_overview():
    """系统概览页面"""
    st.header("🏠 系统概览")

    # 关键指标
    col1, col2, col3, col4 = st.columns(4)

    # 从数据库获取统计数据
    singers_count = 0
    concerts_count = 0
    total_revenue = 0
    city_count = 0

    try:
        singers_df = get_data('singers')
        concerts_df = get_data('concerts')
        cities_df = get_data('cities')

        if not singers_df.empty:
            singers_count = len(singers_df)
        if not concerts_df.empty:
            concerts_count = len(concerts_df)
            if 'revenue' in concerts_df.columns:
                total_revenue = concerts_df['revenue'].sum()
        if not cities_df.empty:
            city_count = len(cities_df)
    except Exception as e:
        print(f"获取统计数据失败: {e}")

    with col1:
        st.metric("🎤 歌手数量", f"{singers_count}")
    with col2:
        st.metric("🎫 演唱会记录", f"{concerts_count}")
    with col3:
        st.metric("🏙️ 覆盖城市", f"{city_count}")
    with col4:
        st.metric("💰 总收入", f"¥{total_revenue:,.0f}")

    st.markdown("---")

    # 功能简介
    st.subheader("系统功能简介")

    features = [
        ("🎤 歌手管理", "添加、编辑、删除歌手信息，管理歌手基础数据"),
        ("🎫 演唱会管理", "记录演唱会详情，包括时间、地点、票务等信息"),
        ("📊 热度分析", "分析歌手热度趋势，粉丝增长情况"),
        ("🏙️ 城市管理", "管理城市数据，分析各城市演唱会市场"),
        ("🔮 预测分析", "预测演唱会需求，推荐最佳举办城市"),
        ("📈 数据可视化", "通过图表直观展示数据分析结果")
    ]

    for icon, desc in features:
        st.markdown(f"**{icon} {desc}**")

    st.markdown("---")

    # 技术架构
    st.subheader("技术架构")
    st.markdown("""
    - **数据库**: SQLite (轻量级文件数据库)
    - **后端**: Python 3.8+
    - **前端**: Streamlit框架
    - **数据可视化**: Plotly, Matplotlib
    - **预测模型**: Scikit-learn
    """)

    # 使用说明
    st.subheader("使用说明")
    st.info("""
    1. 使用左侧导航菜单切换不同功能模块
    2. 在数据展示页面，可以筛选和查看详细数据
    3. 在数据分析页面，可以查看各种可视化图表
    4. 在预测功能页面，可以预测歌手未来热度
    5. 在数据库管理页面，可以直接执行SQL查询
    """)


def show_singer_management():
    """歌手管理页面"""
    st.header("🎤 歌手管理")

    # 获取数据
    singers_df = get_data('singers')

    # 显示数据信息
    if singers_df.empty:
        st.warning("暂无歌手数据，请先初始化数据库")
        return

    tab1, tab2, tab3 = st.tabs(["📋 歌手列表", "➕ 添加歌手", "✏️ 编辑歌手"])

    with tab1:
        st.subheader("所有歌手")

        # 搜索和筛选
        col1, col2 = st.columns(2)
        with col1:
            search_name = st.text_input("搜索歌手姓名", key="search_list")
        with col2:
            if 'genre' in singers_df.columns:
                genre_filter = st.multiselect(
                    "音乐流派筛选",
                    options=singers_df['genre'].unique(),
                    key="genre_filter"
                )
            else:
                genre_filter = []

        # 应用筛选
        filtered_df = singers_df.copy()
        if search_name:
            filtered_df = filtered_df[filtered_df['name'].str.contains(search_name, case=False, na=False)]
        if genre_filter:
            filtered_df = filtered_df[filtered_df['genre'].isin(genre_filter)]

        # 显示数据
        st.dataframe(
            filtered_df,
            column_config={
                "singer_id": "ID",
                "name": "姓名",
                "birth_date": "出生日期",
                "nationality": "国籍",
                "debut_year": "出道年份",
                "genre": "音乐流派",
                "active_status": "活跃状态"
            },
            hide_index=True,
            use_container_width=True
        )

        # 统计信息
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("歌手总数", len(filtered_df))
        with col2:
            if 'active_status' in filtered_df.columns:
                active_count = len(filtered_df[filtered_df['active_status'] == '活跃'])
                st.metric("活跃歌手", active_count)
        with col3:
            if 'nationality' in filtered_df.columns:
                country_count = filtered_df['nationality'].nunique()
                st.metric("国籍数量", country_count)

    with tab2:
        st.subheader("添加新歌手")
        with st.form("add_singer_form"):
            col1, col2 = st.columns(2)

            with col1:
                name = st.text_input("歌手姓名*", placeholder="请输入歌手全名", key="add_name")
                birth_date = st.date_input("出生日期", value=datetime(1990, 1, 1), key="add_birth")
                nationality = st.text_input("国籍", value="中国", key="add_nationality")

            with col2:
                debut_year = st.number_input("出道年份", min_value=1900, max_value=2100, value=2020, key="add_debut")
                genre = st.text_input("音乐流派", placeholder="例如：流行、摇滚、R&B", key="add_genre")
                active_status = st.selectbox("活跃状态", ["活跃", "不活跃"], index=0, key="add_status")

            submitted = st.form_submit_button("🎵 添加歌手", type="primary")

            if submitted:
                if not name:
                    st.error("歌手姓名不能为空！")
                else:
                    # 构建插入SQL
                    sql = """
                        INSERT INTO singers (name, birth_date, nationality, debut_year, genre, active_status)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """

                    params = (
                        name,
                        birth_date.strftime('%Y-%m-%d'),
                        nationality,
                        debut_year,
                        genre,
                        active_status
                    )

                    # 执行插入
                    success = execute_sql(sql, params)

                    if success:
                        st.success(f"歌手 {name} 添加成功！")
                        # 清除缓存，重新加载数据
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("添加失败，请检查数据库连接")

    with tab3:
        st.subheader("编辑歌手信息")

        if not singers_df.empty:
            # 搜索过滤
            col_search, col_info = st.columns([3, 1])

            with col_search:
                search_edit = st.text_input(
                    "🔍 搜索歌手",
                    placeholder="输入歌手姓名搜索",
                    key="search_edit"
                )

            with col_info:
                st.metric("总歌手数", len(singers_df))

            # 过滤歌手列表
            filtered_singers = singers_df.copy()
            if search_edit:
                filtered_singers = singers_df[
                    singers_df['name'].str.contains(search_edit, case=False, na=False)
                ]
                st.caption(f"找到 {len(filtered_singers)} 位符合条件的歌手")

            if filtered_singers.empty:
                st.warning("没有找到符合条件的歌手")
            else:
                # 选择要编辑的歌手
                singer_to_edit = st.selectbox(
                    "选择要编辑的歌手",
                    filtered_singers['name'].tolist(),
                    key="edit_singer_select"
                )

                if singer_to_edit:
                    # 获取选中的歌手信息
                    singer_info = singers_df[singers_df['name'] == singer_to_edit].iloc[0]

                    # 使用容器分隔显示
                    st.divider()

                    col_left, col_right = st.columns([3, 2])

                    with col_left:
                        st.markdown(f"### 📝 编辑 {singer_to_edit}")

                        with st.form("edit_singer_form"):
                            col1, col2 = st.columns(2)

                            with col1:
                                edit_name = st.text_input(
                                    "歌手姓名*",
                                    value=singer_info.get('name', ''),
                                    placeholder="请输入歌手全名",
                                    key="edit_name"
                                )

                                # 处理出生日期
                                birth_date_value = singer_info.get('birth_date')
                                if pd.notna(birth_date_value) and birth_date_value:
                                    try:
                                        edit_birth_date = st.date_input(
                                            "出生日期",
                                            value=pd.to_datetime(birth_date_value),
                                            key="edit_birth"
                                        )
                                    except:
                                        edit_birth_date = st.date_input(
                                            "出生日期",
                                            value=datetime(1990, 1, 1),
                                            key="edit_birth_alt"
                                        )
                                else:
                                    edit_birth_date = st.date_input(
                                        "出生日期",
                                        value=datetime(1990, 1, 1),
                                        key="edit_birth_default"
                                    )

                                edit_nationality = st.text_input(
                                    "国籍",
                                    value=singer_info.get('nationality', '中国'),
                                    key="edit_nationality"
                                )

                            with col2:
                                edit_debut_year = st.number_input(
                                    "出道年份",
                                    min_value=1900,
                                    max_value=2100,
                                    value=int(singer_info.get('debut_year', 2020)),
                                    key="edit_debut"
                                )

                                edit_genre = st.text_input(
                                    "音乐流派",
                                    value=singer_info.get('genre', ''),
                                    placeholder="例如：流行、摇滚、R&B",
                                    key="edit_genre"
                                )

                                edit_active_status = st.selectbox(
                                    "活跃状态",
                                    ["活跃", "不活跃"],
                                    index=0 if singer_info.get('active_status') == '活跃' else 1,
                                    key="edit_status"
                                )

                            # 隐藏字段：存储歌手ID
                            singer_id = singer_info.get('singer_id')

                            col_btn1, col_btn2 = st.columns(2)
                            with col_btn1:
                                submitted = st.form_submit_button("💾 保存修改", type="primary",
                                                                  use_container_width=True)
                            with col_btn2:
                                cancel_clicked = st.form_submit_button("❌ 取消", use_container_width=True)

                            if submitted:
                                if not edit_name:
                                    st.error("歌手姓名不能为空！")
                                else:
                                    # 构建更新SQL
                                    sql = """
                                        UPDATE singers 
                                        SET name = ?, 
                                            birth_date = ?, 
                                            nationality = ?, 
                                            debut_year = ?, 
                                            genre = ?, 
                                            active_status = ?
                                        WHERE singer_id = ?
                                    """

                                    params = (
                                        edit_name,
                                        edit_birth_date.strftime('%Y-%m-%d'),
                                        edit_nationality,
                                        edit_debut_year,
                                        edit_genre,
                                        edit_active_status,
                                        singer_id
                                    )

                                    # 执行更新
                                    success = execute_sql(sql, params)

                                    if success:
                                        st.success(f"歌手 {edit_name} 信息更新成功！")
                                        # 清除缓存，重新加载数据
                                        st.cache_data.clear()
                                        # 添加延迟，确保用户看到成功消息
                                        import time
                                        time.sleep(1.5)
                                        st.rerun()
                                    else:
                                        st.error("更新失败，请检查数据库连接")

                            if cancel_clicked:
                                st.info("编辑已取消")

                    with col_right:
                        st.markdown("### 📋 当前信息")

                        # 显示当前信息卡片
                        with st.container():
                            st.markdown(f"""
                            <div style="background-color: #f0f2f6; padding: 15px; border-radius: 10px; margin-bottom: 10px;">
                                <strong>ID:</strong> {singer_info.get('singer_id')}<br>
                                <strong>姓名:</strong> {singer_info.get('name')}<br>
                                <strong>国籍:</strong> {singer_info.get('nationality')}<br>
                                <strong>流派:</strong> {singer_info.get('genre')}<br>
                                <strong>状态:</strong> {singer_info.get('active_status')}<br>
                                <strong>出道年份:</strong> {singer_info.get('debut_year')}<br>
                                <strong>出生日期:</strong> {singer_info.get('birth_date')}
                            </div>
                            """, unsafe_allow_html=True)

                        st.divider()

                        # 添加删除功能
                        st.markdown("### ⚠️ 危险操作")

                        with st.expander("删除该歌手", icon="🗑️"):
                            st.warning("删除操作不可恢复，请谨慎操作！")

                            # 确认删除机制
                            delete_confirmed = st.checkbox("我确认要删除此歌手", key=f"delete_confirm_{singer_id}")

                            if delete_confirmed:
                                # 额外确认
                                confirm_text = st.text_input(
                                    f"请输入 '{singer_to_edit}' 确认删除",
                                    key=f"delete_text_{singer_id}"
                                )

                                col_del1, col_del2 = st.columns(2)
                                with col_del1:
                                    delete_clicked = st.button(
                                        "🗑️ 确认删除",
                                        type="secondary",
                                        disabled=(confirm_text != singer_to_edit),
                                        use_container_width=True,
                                        key=f"delete_btn_{singer_id}"
                                    )

                                with col_del2:
                                    cancel_delete = st.button(
                                        "取消删除",
                                        use_container_width=True,
                                        key=f"cancel_delete_{singer_id}"
                                    )

                                if delete_clicked and confirm_text == singer_to_edit:
                                    delete_sql = "DELETE FROM singers WHERE singer_id = ?"
                                    if execute_sql(delete_sql, (singer_id,)):
                                        st.success("歌手已成功删除")
                                        # 清除缓存，重新加载数据
                                        st.cache_data.clear()
                                        import time
                                        time.sleep(1.5)
                                        st.rerun()
                                    else:
                                        st.error("删除失败，请检查数据库连接")

                                if cancel_delete:
                                    st.info("删除操作已取消")


def show_concert_management():
    """演唱会管理页面"""
    st.header("🎫 演唱会管理")

    # 获取数据
    concerts_df = get_data('concerts')
    singers_df = get_data('singers')

    if concerts_df.empty or singers_df.empty:
        st.warning("暂无演唱会数据，请先初始化数据库")
        return

    tab1, tab2, tab3 = st.tabs(["📋 演唱会列表", "➕ 添加演唱会", "📊 演唱会统计"])

    with tab1:
        st.subheader("所有演唱会")

        # 合并数据前确保数据类型正确
        concerts_df = concerts_df.copy()
        singers_df = singers_df.copy()

        # 确保数值列是数值类型
        numeric_cols_concerts = ['singer_id', 'capacity', 'attendance', 'ticket_price', 'revenue', 'attendance_rate']
        for col in numeric_cols_concerts:
            if col in concerts_df.columns:
                concerts_df[col] = pd.to_numeric(concerts_df[col], errors='coerce')

        if 'singer_id' in singers_df.columns:
            singers_df['singer_id'] = pd.to_numeric(singers_df['singer_id'], errors='coerce')

        merged_data = pd.merge(concerts_df, singers_df,
                               left_on='singer_id', right_on='singer_id',
                               how='left')

        # 确保合并后的字符串列是字符串类型
        string_cols = ['concert_name', 'city', 'venue', 'name']
        for col in string_cols:
            if col in merged_data.columns:
                merged_data[col] = merged_data[col].astype(str)

        # 筛选选项
        col1, col2, col3 = st.columns(3)
        with col1:
            singer_options = ["全部"] + list(merged_data['name'].unique())
            singer_filter = st.selectbox("选择歌手", options=singer_options)
        with col2:
            city_options = ["全部"] + list(merged_data['city'].unique())
            city_filter = st.selectbox("选择城市", options=city_options)
        with col3:
            if 'concert_date' in merged_data.columns:
                merged_data['concert_date'] = pd.to_datetime(merged_data['concert_date'])
                year_options = ["全部"] + sorted(list(merged_data['concert_date'].dt.year.unique()), reverse=True)
                year_filter = st.selectbox("选择年份", options=year_options)
            else:
                year_filter = "全部"

        # 应用筛选
        filtered_data = merged_data.copy()
        if singer_filter != "全部":
            filtered_data = filtered_data[filtered_data['name'] == singer_filter]
        if city_filter != "全部":
            filtered_data = filtered_data[filtered_data['city'] == city_filter]
        if year_filter != "全部" and 'concert_date' in filtered_data.columns:
            filtered_data = filtered_data[filtered_data['concert_date'].dt.year == int(year_filter)]

        # 显示数据
        if not filtered_data.empty:
            display_cols = []
            for col in ['concert_name', 'name', 'concert_date', 'city', 'venue',
                        'capacity', 'attendance', 'ticket_price', 'revenue', 'attendance_rate']:
                if col in filtered_data.columns:
                    display_cols.append(col)

            # 确保显示的数据类型正确
            display_df = filtered_data[display_cols].copy()

            # 确保数值列格式正确
            if 'capacity' in display_df.columns:
                display_df['capacity'] = pd.to_numeric(display_df['capacity'], errors='coerce')
            if 'attendance' in display_df.columns:
                display_df['attendance'] = pd.to_numeric(display_df['attendance'], errors='coerce')
            if 'ticket_price' in display_df.columns:
                display_df['ticket_price'] = pd.to_numeric(display_df['ticket_price'], errors='coerce')
            if 'revenue' in display_df.columns:
                display_df['revenue'] = pd.to_numeric(display_df['revenue'], errors='coerce')
            if 'attendance_rate' in display_df.columns:
                display_df['attendance_rate'] = pd.to_numeric(display_df['attendance_rate'], errors='coerce')

            st.dataframe(
                display_df,
                column_config={
                    "concert_name": "演唱会名称",
                    "name": "歌手",
                    "concert_date": "日期",
                    "city": "城市",
                    "venue": "场馆",
                    "capacity": "容量",
                    "attendance": "出席人数",
                    "ticket_price": st.column_config.NumberColumn("票价", format="¥%.2f"),
                    "revenue": st.column_config.NumberColumn("收入", format="¥%.2f"),
                    "attendance_rate": st.column_config.NumberColumn("上座率", format="%.2f")
                },
                hide_index=True,
                use_container_width=True
            )

            # 统计信息
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("演唱会总数", len(filtered_data))
            with col2:
                total_revenue = filtered_data['revenue'].sum() if 'revenue' in filtered_data.columns else 0
                st.metric("总收入", f"¥{total_revenue:,.0f}")
            with col3:
                if 'attendance' in filtered_data.columns:
                    avg_attendance = filtered_data['attendance'].mean()
                    st.metric("平均出席人数", f"{avg_attendance:,.0f}")
            with col4:
                if 'attendance_rate' in filtered_data.columns:
                    avg_rate = filtered_data['attendance_rate'].mean() * 100
                    st.metric("平均上座率", f"{avg_rate:.1f}%")
        else:
            st.warning("没有找到符合条件的演唱会记录")

    with tab2:
        st.subheader("添加新演唱会")
        if not singers_df.empty:
            with st.form("add_concert_form"):
                col1, col2 = st.columns(2)

                with col1:
                    singer_name = st.selectbox(
                        "选择歌手",
                        options=singers_df['name'].tolist()
                    )
                    concert_name = st.text_input("演唱会名称*", placeholder="例如：2024世界巡回演唱会")
                    concert_date = st.date_input("演唱会日期", value=datetime.now())
                    city = st.text_input("城市*", placeholder="例如：北京")

                with col2:
                    venue = st.text_input("场馆", placeholder="例如：国家体育场")
                    capacity = st.number_input("场馆容量", min_value=100, max_value=100000, value=10000)
                    attendance = st.number_input("实际出席人数", min_value=0, max_value=100000, value=8000)
                    ticket_price = st.number_input("票价(元)", min_value=0, max_value=10000, value=500)

                submitted = st.form_submit_button("🎫 添加演唱会", type="primary")

                if submitted:
                    if not concert_name or not city:
                        st.error("演唱会名称和城市不能为空！")
                    else:
                        # 获取歌手ID
                        singer_id = singers_df[singers_df['name'] == singer_name]['singer_id'].iloc[0]

                        # 计算收入
                        revenue = attendance * ticket_price
                        attendance_rate = attendance / capacity if capacity > 0 else 0

                        # 构建插入SQL
                        sql = """
                            INSERT INTO concerts 
                            (singer_id, concert_name, concert_date, city, venue, capacity, 
                             attendance, ticket_price, revenue, attendance_rate)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """

                        params = (
                            singer_id,
                            concert_name,
                            concert_date.strftime('%Y-%m-%d'),
                            city,
                            venue,
                            capacity,
                            attendance,
                            ticket_price,
                            revenue,
                            attendance_rate
                        )

                        # 执行插入
                        success = execute_sql(sql, params)

                        if success:
                            st.success(f"演唱会 {concert_name} 添加成功！")
                            # 清除缓存，重新加载数据
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("添加失败，请检查数据库连接")

    with tab3:
        st.subheader("演唱会统计")

        if not concerts_df.empty and not singers_df.empty:
            merged_data = pd.merge(concerts_df, singers_df,
                                   left_on='singer_id', right_on='singer_id',
                                   how='left')

            # 按歌手统计
            singer_stats = merged_data.groupby('name').agg({
                'concert_id': 'count',
                'revenue': 'sum',
                'attendance': 'sum',
                'attendance_rate': 'mean'
            }).reset_index()

            singer_stats = singer_stats.rename(columns={
                'concert_id': '演唱会场次',
                'revenue': '总收入',
                'attendance': '总观众数',
                'attendance_rate': '平均上座率'
            })

            singer_stats['平均上座率'] = singer_stats['平均上座率'] * 100

            # 显示歌手统计
            st.markdown("#### 📊 按歌手统计")
            st.dataframe(
                singer_stats.sort_values('总收入', ascending=False),
                column_config={
                    "name": "歌手",
                    "演唱会场次": "场次",
                    "总收入": st.column_config.NumberColumn("总收入", format="¥%.0f"),
                    "总观众数": "观众数",
                    "平均上座率": st.column_config.NumberColumn("上座率", format="%.1f%%")
                },
                hide_index=True,
                use_container_width=True
            )

            # 按城市统计
            if 'city' in merged_data.columns:
                city_stats = merged_data.groupby('city').agg({
                    'concert_id': 'count',
                    'revenue': 'sum',
                    'attendance': 'sum'
                }).reset_index()

                city_stats = city_stats.rename(columns={
                    'concert_id': '演唱会场次',
                    'revenue': '总收入',
                    'attendance': '总观众数'
                })

                st.markdown("#### 🏙️ 按城市统计")
                st.dataframe(
                    city_stats.sort_values('总收入', ascending=False),
                    column_config={
                        "city": "城市",
                        "演唱会场次": "场次",
                        "总收入": st.column_config.NumberColumn("总收入", format="¥%.0f"),
                        "总观众数": "观众数"
                    },
                    hide_index=True,
                    use_container_width=True
                )


def show_popularity_analysis():
    """热度分析页面"""
    st.header("📊 热度分析")

    # 获取数据
    singers_df = get_data('singers')
    popularity_df = get_data('popularity')

    if singers_df.empty:
        st.warning("暂无歌手数据，请先初始化数据库")
        return

    if popularity_df.empty:
        st.warning("暂无热度数据，请先初始化数据库")
        return

    # 选择歌手
    selected_singer = st.selectbox(
        "选择歌手",
        singers_df['name'].unique()
    )

    if selected_singer:
        # 获取歌手ID
        singer_id = singers_df[singers_df['name'] == selected_singer]['singer_id'].iloc[0]

        # 获取该歌手的热度数据
        singer_popularity = popularity_df[popularity_df['singer_id'] == singer_id].copy()

        if not singer_popularity.empty:
            # 转换为日期格式
            singer_popularity['record_date'] = pd.to_datetime(singer_popularity['record_date'])
            singer_popularity = singer_popularity.sort_values('record_date')

            # 显示最新数据
            latest = singer_popularity.iloc[-1]

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                if 'fan_count' in latest:
                    st.metric("当前粉丝量", f"{latest['fan_count']:,}")
                else:
                    st.metric("当前粉丝量", "N/A")
            with col2:
                if 'topic_score' in latest:
                    st.metric("话题度", f"{latest['topic_score']:.1f}")
                else:
                    st.metric("话题度", "N/A")
            with col3:
                if 'popularity_score' in latest:
                    st.metric("传唱度", f"{latest['popularity_score']:.1f}")
                else:
                    st.metric("传唱度", "N/A")
            with col4:
                if 'social_media_mentions' in latest:
                    st.metric("社交媒体提及", f"{latest['social_media_mentions']:,}")
                else:
                    st.metric("社交媒体提及", "N/A")

            # 热度趋势图
            st.subheader("热度趋势")

            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=("粉丝量变化", "话题度变化", "传唱度变化", "综合热度"),
                vertical_spacing=0.15
            )

            # 粉丝量
            if 'fan_count' in singer_popularity.columns:
                fig.add_trace(
                    go.Scatter(x=singer_popularity['record_date'], y=singer_popularity['fan_count'],
                               mode='lines+markers', name='粉丝量'),
                    row=1, col=1
                )

            # 话题度
            if 'topic_score' in singer_popularity.columns:
                fig.add_trace(
                    go.Scatter(x=singer_popularity['record_date'], y=singer_popularity['topic_score'],
                               mode='lines+markers', name='话题度'),
                    row=1, col=2
                )

            # 传唱度
            if 'popularity_score' in singer_popularity.columns:
                fig.add_trace(
                    go.Scatter(x=singer_popularity['record_date'], y=singer_popularity['popularity_score'],
                               mode='lines+markers', name='传唱度'),
                    row=2, col=1
                )

            # 综合热度
            if 'topic_score' in singer_popularity.columns and 'popularity_score' in singer_popularity.columns:
                singer_popularity['composite_score'] = (singer_popularity['topic_score'] + singer_popularity[
                    'popularity_score']) / 2
                fig.add_trace(
                    go.Scatter(x=singer_popularity['record_date'], y=singer_popularity['composite_score'],
                               mode='lines+markers', name='综合热度'),
                    row=2, col=2
                )

            fig.update_layout(height=600, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

            # 显示详细数据表
            st.subheader("详细数据")
            display_cols = ['record_date']

            for col in ['fan_count', 'topic_score', 'popularity_score', 'social_media_mentions']:
                if col in singer_popularity.columns:
                    display_cols.append(col)

            st.dataframe(
                singer_popularity[display_cols],
                column_config={
                    "record_date": "记录日期",
                    "fan_count": "粉丝量",
                    "topic_score": "话题度",
                    "popularity_score": "传唱度",
                    "social_media_mentions": "社交媒体提及"
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.warning("该歌手没有热度数据")


def show_city_management():
    """城市管理页面"""
    st.header("🏙️ 城市管理")

    # 获取数据
    cities_df = get_data('cities')

    if cities_df.empty:
        st.warning("暂无城市数据，请先初始化数据库")
        return

    tab1, tab2 = st.tabs(["📋 城市列表", "➕ 添加城市"])

    with tab1:
        st.subheader("所有城市")

        # 显示数据
        st.dataframe(
            cities_df,
            column_config={
                "city_id": "城市ID",
                "city_name": "城市名称",
                "country": "国家",
                "population": "人口(万)",
                "avg_concert_capacity": "平均演唱会容量",
                "concert_frequency": "每月平均演唱会次数"
            },
            hide_index=True,
            use_container_width=True
        )

        # 可视化
        col1, col2 = st.columns(2)

        with col1:
            # 人口分布图
            fig1 = px.bar(
                cities_df.sort_values('population', ascending=False),
                x='city_name',
                y='population',
                title="城市人口分布",
                color='population',
                color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            # 演唱会频率图
            fig2 = px.bar(
                cities_df.sort_values('concert_frequency', ascending=False),
                x='city_name',
                y='concert_frequency',
                title="每月平均演唱会次数",
                color='concert_frequency',
                color_continuous_scale='Plasma'
            )
            st.plotly_chart(fig2, use_container_width=True)

    with tab2:
        st.subheader("添加新城市")
        with st.form("add_city_form"):
            col1, col2 = st.columns(2)

            with col1:
                city_name = st.text_input("城市名称*", placeholder="例如：北京")
                country = st.text_input("国家", value="中国")
                population = st.number_input("人口(万)", min_value=0, value=1000)

            with col2:
                avg_concert_capacity = st.number_input("平均演唱会容量", min_value=0, value=10000)
                concert_frequency = st.number_input("每月平均演唱会次数", min_value=0, value=10)

            submitted = st.form_submit_button("🏙️ 添加城市", type="primary")

            if submitted:
                if not city_name:
                    st.error("城市名称不能为空！")
                else:
                    # 构建插入SQL
                    sql = """
                        INSERT INTO cities (city_name, country, population, avg_concert_capacity, concert_frequency)
                        VALUES (?, ?, ?, ?, ?)
                    """

                    params = (
                        city_name,
                        country,
                        population,
                        avg_concert_capacity,
                        concert_frequency
                    )

                    # 执行插入
                    success = execute_sql(sql, params)

                    if success:
                        st.success(f"城市 {city_name} 添加成功！")
                        # 清除缓存，重新加载数据
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("添加失败，请检查数据库连接")


def show_default_prediction_chart(selected_singer, future_months):
    """显示默认预测图表（当数据不足时）"""
    # 从数据库获取真实数据
    singers_df = get_data('singers')
    popularity_df = get_data('popularity')

    if singers_df.empty or popularity_df.empty:
        st.warning("没有足够的数据进行预测")
        return

    singer_id = singers_df[singers_df['name'] == selected_singer]['singer_id'].iloc[0]
    singer_popularity = popularity_df[popularity_df['singer_id'] == singer_id].copy()

    if len(singer_popularity) < 3:
        st.warning(f"{selected_singer} 的历史数据不足，至少需要3个月的数据才能进行预测")
        return

    # 转换为日期格式
    singer_popularity['record_date'] = pd.to_datetime(singer_popularity['record_date'])
    singer_popularity = singer_popularity.sort_values('record_date')

    # 基于真实数据的预测
    if 'fan_count' in singer_popularity.columns:
        fan_counts = singer_popularity['fan_count'].values
        dates = singer_popularity['record_date'].values

        # 计算历史增长率
        if len(fan_counts) >= 2:
            growth_rates = []
            for i in range(1, len(fan_counts)):
                if fan_counts[i - 1] > 0:
                    growth_rate = (fan_counts[i] - fan_counts[i - 1]) / fan_counts[i - 1]
                    growth_rates.append(growth_rate)

            if growth_rates:
                avg_growth_rate = np.mean(growth_rates)
                # 限制增长率范围在合理区间
                avg_growth_rate = max(min(avg_growth_rate, 0.2), -0.1)

                # 生成预测数据
                last_date = dates[-1]
                last_fan_count = fan_counts[-1]

                # 预测数据点
                future_dates = []
                future_fan_counts = []

                for i in range(1, future_months + 1):
                    next_date = last_date + pd.DateOffset(months=i)
                    # 添加随机波动，使预测更真实
                    random_factor = np.random.uniform(0.95, 1.05)
                    next_fan_count = int(last_fan_count * (1 + avg_growth_rate) ** i * random_factor)
                    future_dates.append(next_date)
                    future_fan_counts.append(next_fan_count)

                # 创建图表
                fig = go.Figure()

                fig.add_trace(go.Scatter(
                    x=dates,
                    y=fan_counts,
                    mode='lines+markers',
                    name='历史粉丝量',
                    line=dict(color='blue', width=2)
                ))

                fig.add_trace(go.Scatter(
                    x=future_dates,
                    y=future_fan_counts,
                    mode='lines+markers',
                    name='预测粉丝量',
                    line=dict(color='red', width=2, dash='dash')
                ))

                fig.update_layout(
                    title=f"{selected_singer}粉丝量预测",
                    xaxis_title="日期",
                    yaxis_title="粉丝量",
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

                # 预测增长率
                predicted_growth = ((future_fan_counts[-1] - fan_counts[-1]) / fan_counts[-1]) * 100 if fan_counts[
                                                                                                            -1] > 0 else 0

                # 给出建议
                if predicted_growth > 15:
                    st.success(f"📈 预测粉丝量增长{predicted_growth:.1f}%，建议增加宣传和演出场次")
                elif predicted_growth > 5:
                    st.info(f"📊 预测粉丝量增长{predicted_growth:.1f}%，建议维持当前策略")
                else:
                    st.warning(f"📉 预测粉丝量增长{predicted_growth:.1f}%，建议加强粉丝互动和内容创作")


def show_prediction_analysis():
    """预测分析页面"""
    st.header("🔮 预测分析")

    tab1, tab2 = st.tabs(["📈 热度走势预测", "📍 城市推荐"])

    with tab1:
        st.subheader("歌手未来热度走势预测")

        # 获取数据
        singers_df = get_data('singers')
        popularity_df = get_data('popularity')

        if singers_df.empty or popularity_df.empty:
            st.warning("暂无数据用于预测，请先初始化数据库")
            return

        # 选择歌手
        selected_singer = st.selectbox(
            "选择要预测的歌手",
            singers_df['name'].unique(),
            key="predict_singer"
        )

        if selected_singer:
            # 获取歌手ID
            singer_id = singers_df[singers_df['name'] == selected_singer]['singer_id'].iloc[0]

            # 获取该歌手的历史热度数据
            singer_popularity = popularity_df[popularity_df['singer_id'] == singer_id].copy()

            if not singer_popularity.empty:
                # 按日期排序
                singer_popularity['record_date'] = pd.to_datetime(singer_popularity['record_date'])
                singer_popularity = singer_popularity.sort_values('record_date')

                st.info(f"正在分析 {selected_singer} 的热度趋势...")

                # 预测未来月数
                future_months = st.slider("预测未来月数", 1, 12, 3)

                # ================ 基于实际数据的预测 ================

                # 1. 粉丝量预测（基于历史增长趋势）
                if len(singer_popularity) >= 3:  # 至少有3个数据点才做预测
                    fan_counts = singer_popularity['fan_count'].values
                    dates = singer_popularity['record_date'].values

                    # 计算历史增长率
                    if len(fan_counts) >= 2:
                        # 计算月度增长率
                        growth_rates = []
                        for i in range(1, len(fan_counts)):
                            if fan_counts[i - 1] > 0:
                                growth_rate = (fan_counts[i] - fan_counts[i - 1]) / fan_counts[i - 1]
                                growth_rates.append(growth_rate)

                        if growth_rates:
                            avg_growth_rate = np.mean(growth_rates)
                            # 限制增长率范围在合理区间
                            avg_growth_rate = max(min(avg_growth_rate, 0.2), -0.1)

                            # 生成预测数据
                            last_date = dates[-1]
                            last_fan_count = fan_counts[-1]

                            # 历史数据点
                            historical_dates = dates
                            historical_fan_counts = fan_counts

                            # 预测数据点
                            future_dates = []
                            future_fan_counts = []

                            for i in range(1, future_months + 1):
                                next_date = last_date + pd.DateOffset(months=i)
                                # 添加随机波动，使预测更真实
                                random_factor = np.random.uniform(0.95, 1.05)
                                next_fan_count = int(last_fan_count * (1 + avg_growth_rate) ** i * random_factor)
                                future_dates.append(next_date)
                                future_fan_counts.append(next_fan_count)

                            # 2. 热度评分预测
                            if 'topic_score' in singer_popularity.columns and 'popularity_score' in singer_popularity.columns:
                                topic_scores = singer_popularity['topic_score'].values
                                popularity_scores = singer_popularity['popularity_score'].values

                                topic_avg = np.mean(topic_scores)
                                popularity_avg = np.mean(popularity_scores)

                                # 预测未来热度
                                future_topic_scores = []
                                future_popularity_scores = []

                                for i in range(future_months):
                                    # 基于平均值，添加随机波动
                                    topic_random = np.random.uniform(0.95, 1.05)
                                    popularity_random = np.random.uniform(0.95, 1.05)
                                    future_topic_scores.append(topic_avg * topic_random)
                                    future_popularity_scores.append(popularity_avg * popularity_random)

                            # ================ 显示预测图表 ================

                            # 创建子图
                            fig = make_subplots(
                                rows=2, cols=2,
                                subplot_titles=("粉丝量预测", "话题度预测", "传唱度预测", "综合热度预测"),
                                vertical_spacing=0.15
                            )

                            # 粉丝量预测图
                            fig.add_trace(
                                go.Scatter(
                                    x=historical_dates,
                                    y=historical_fan_counts,
                                    mode='lines+markers',
                                    name='历史粉丝量',
                                    line=dict(color='blue', width=2)
                                ),
                                row=1, col=1
                            )

                            fig.add_trace(
                                go.Scatter(
                                    x=future_dates,
                                    y=future_fan_counts,
                                    mode='lines+markers',
                                    name='预测粉丝量',
                                    line=dict(color='red', width=2, dash='dash')
                                ),
                                row=1, col=1
                            )

                            # 话题度预测图
                            if 'future_topic_scores' in locals():
                                fig.add_trace(
                                    go.Scatter(
                                        x=historical_dates,
                                        y=topic_scores,
                                        mode='lines+markers',
                                        name='历史话题度',
                                        line=dict(color='green', width=2)
                                    ),
                                    row=1, col=2
                                )

                                fig.add_trace(
                                    go.Scatter(
                                        x=future_dates,
                                        y=future_topic_scores,
                                        mode='lines+markers',
                                        name='预测话题度',
                                        line=dict(color='orange', width=2, dash='dash')
                                    ),
                                    row=1, col=2
                                )

                            # 传唱度预测图
                            if 'future_popularity_scores' in locals():
                                fig.add_trace(
                                    go.Scatter(
                                        x=historical_dates,
                                        y=popularity_scores,
                                        mode='lines+markers',
                                        name='历史传唱度',
                                        line=dict(color='purple', width=2)
                                    ),
                                    row=2, col=1
                                )

                                fig.add_trace(
                                    go.Scatter(
                                        x=future_dates,
                                        y=future_popularity_scores,
                                        mode='lines+markers',
                                        name='预测传唱度',
                                        line=dict(color='brown', width=2, dash='dash')
                                    ),
                                    row=2, col=1
                                )

                            # 综合热度预测图
                            if 'future_topic_scores' in locals() and 'future_popularity_scores' in locals():
                                historical_composite = (topic_scores + popularity_scores) / 2
                                future_composite = (np.array(future_topic_scores) + np.array(
                                    future_popularity_scores)) / 2

                                fig.add_trace(
                                    go.Scatter(
                                        x=historical_dates,
                                        y=historical_composite,
                                        mode='lines+markers',
                                        name='历史综合热度',
                                        line=dict(color='darkblue', width=2)
                                    ),
                                    row=2, col=2
                                )

                                fig.add_trace(
                                    go.Scatter(
                                        x=future_dates,
                                        y=future_composite,
                                        mode='lines+markers',
                                        name='预测综合热度',
                                        line=dict(color='darkred', width=2, dash='dash')
                                    ),
                                    row=2, col=2
                                )

                            fig.update_layout(height=600, showlegend=True)
                            st.plotly_chart(fig, use_container_width=True)

                            # ================ 提供个性化建议 ================
                            st.subheader("📊 分析报告")

                            col1, col2, col3 = st.columns(3)

                            with col1:
                                # 计算增长率
                                if len(fan_counts) >= 2:
                                    current_growth = ((fan_counts[-1] - fan_counts[0]) / fan_counts[0]) * 100 if \
                                    fan_counts[0] > 0 else 0
                                    st.metric("历史增长率", f"{current_growth:.1f}%")

                            with col2:
                                # 预测增长率
                                predicted_growth = ((future_fan_counts[-1] - fan_counts[-1]) / fan_counts[-1]) * 100 if \
                                fan_counts[-1] > 0 else 0
                                st.metric("预测增长率", f"{predicted_growth:.1f}%")

                            with col3:
                                # 热度稳定性
                                if 'topic_scores' in locals() and len(topic_scores) >= 2:
                                    topic_std = np.std(topic_scores)
                                    if topic_std < 5:
                                        stability = "高"
                                    elif topic_std < 10:
                                        stability = "中"
                                    else:
                                        stability = "低"
                                    st.metric("热度稳定性", stability)

                            # 根据歌手特点给出不同建议
                            st.subheader("💡 投资建议")

                            # 获取歌手信息
                            singer_info = singers_df[singers_df['name'] == selected_singer].iloc[0]
                            genre = singer_info.get('genre', '未知')
                            active_status = singer_info.get('active_status', '未知')

                            # 基础分析
                            advice_parts = []

                            # 1. 基于增长率
                            if predicted_growth > 15:
                                advice_parts.append("粉丝量预计大幅增长")
                            elif predicted_growth > 5:
                                advice_parts.append("粉丝量预计稳步增长")
                            elif predicted_growth > 0:
                                advice_parts.append("粉丝量预计缓慢增长")
                            else:
                                advice_parts.append("粉丝量可能出现下滑")

                            # 2. 基于音乐流派
                            if '流行' in genre:
                                advice_parts.append("流行音乐市场接受度高")
                            elif '摇滚' in genre:
                                advice_parts.append("摇滚乐粉丝忠诚度高")
                            elif 'R&B' in genre:
                                advice_parts.append("R&B音乐有稳定的受众群体")

                            # 3. 基于活跃状态
                            if active_status == '活跃':
                                advice_parts.append("歌手当前活跃，曝光机会多")
                            else:
                                advice_parts.append("歌手当前不活跃，需关注复出计划")

                            # 4. 基于历史数据量
                            data_points = len(singer_popularity)
                            if data_points >= 6:
                                advice_parts.append("历史数据充足，预测可信度高")
                            elif data_points >= 3:
                                advice_parts.append("历史数据有限，预测仅供参考")
                            else:
                                advice_parts.append("历史数据不足，建议谨慎投资")

                            # 组合建议
                            if predicted_growth > 10:
                                st.success(f"📈 **强烈推荐投资**\n\n"
                                           f"原因分析：{'，'.join(advice_parts)}\n\n"
                                           f"建议措施：增加演唱会场次，加大宣传力度，考虑品牌代言合作")
                            elif predicted_growth > 0:
                                st.info(f"📊 **谨慎推荐投资**\n\n"
                                        f"原因分析：{'，'.join(advice_parts)}\n\n"
                                        f"建议措施：维持当前策略，关注市场变化，适时调整宣传方向")
                            else:
                                st.warning(f"📉 **建议观望**\n\n"
                                           f"原因分析：{'，'.join(advice_parts)}\n\n"
                                           f"建议措施：加强粉丝互动，提升作品质量，考虑跨界合作")

                            # 显示预测数据
                            with st.expander("📋 查看详细预测数据"):
                                prediction_data = {
                                    '月份': [f"未来第{i}个月" for i in range(1, future_months + 1)],
                                    '预测粉丝量': future_fan_counts,
                                    '预测话题度': future_topic_scores if 'future_topic_scores' in locals() else ['N/A'] * future_months,
                                    '预测传唱度': future_popularity_scores if 'future_popularity_scores' in locals() else ['N/A'] * future_months
                                }
                                prediction_df = pd.DataFrame(prediction_data)
                                st.dataframe(prediction_df, use_container_width=True)

                        else:
                            st.warning("粉丝量数据不足，无法计算增长率")
                    else:
                        st.warning("需要至少2个历史数据点才能进行预测")
                else:
                    st.warning("历史数据不足，至少需要3个月的数据才能进行预测")
            else:
                st.warning(f"没有找到 {selected_singer} 的历史热度数据")

    with tab2:
        st.subheader("适配开办城市推荐")

        # 获取数据
        singers_df = get_data('singers')
        cities_df = get_data('cities')
        concerts_df = get_data('concerts')

        if singers_df.empty or cities_df.empty:
            st.warning("暂无数据用于城市推荐，请先初始化数据库")
            return

        # 选择歌手
        selected_singer = st.selectbox(
            "选择要推荐城市的歌手",
            singers_df['name'].unique(),
            key="city_singer"
        )

        if selected_singer:
            # 获取歌手ID和信息
            singer_id = singers_df[singers_df['name'] == selected_singer]['singer_id'].iloc[0]
            singer_info = singers_df[singers_df['name'] == selected_singer].iloc[0]
            singer_genre = singer_info.get('genre', '流行')

            st.info(f"正在为 {selected_singer} ({singer_genre}) 推荐最佳举办城市...")

            # 获取该歌手的历史演唱会数据
            singer_concerts = concerts_df[
                concerts_df['singer_id'] == singer_id].copy() if not concerts_df.empty else pd.DataFrame()

            # 个性化推荐算法
            recommendations = []

            # 获取所有城市
            all_cities = cities_df.copy()

            for _, city_row in all_cities.iterrows():
                city_name = city_row['city_name']
                population = city_row['population']
                avg_capacity = city_row['avg_concert_capacity']
                frequency = city_row['concert_frequency']

                # 基础得分
                score = 50  # 基础分

                # 1. 人口因素（人口越多，得分越高）
                population_score = min(population / 50, 20)  # 每50万人口加1分，最高20分
                score += population_score

                # 2. 演唱会频率因素（频率适中最好）
                if 5 <= frequency <= 15:
                    frequency_score = 10
                elif frequency < 5:
                    frequency_score = frequency  # 频率太低不好
                else:
                    frequency_score = 20 - frequency  # 频率太高竞争激烈

                score += frequency_score

                # 3. 场馆容量因素
                capacity_score = min(avg_capacity / 2000, 10)  # 每2000容量加1分，最高10分
                score += capacity_score

                # 4. 历史表现因素（如果该歌手在该城市有过演出）
                if not singer_concerts.empty:
                    city_performance = singer_concerts[singer_concerts['city'] == city_name]
                    if not city_performance.empty:
                        # 计算平均上座率
                        avg_attendance_rate = city_performance[
                            'attendance_rate'].mean() if 'attendance_rate' in city_performance.columns else 0.8
                        performance_score = avg_attendance_rate * 20  # 最高20分
                        score += performance_score

                # 5. 音乐流派匹配因素
                genre_bonus = 0
                if '流行' in singer_genre and city_name in ['北京', '上海', '广州', '深圳']:
                    genre_bonus = 10  # 流行音乐在一线城市更受欢迎
                elif '摇滚' in singer_genre and city_name in ['成都', '武汉', '南京']:
                    genre_bonus = 8  # 摇滚音乐在新一线城市有市场
                elif '民谣' in singer_genre and city_name in ['杭州', '西安', '重庆']:
                    genre_bonus = 7  # 民谣音乐在文化城市更受欢迎

                score += genre_bonus

                # 6. 竞争程度因素（演唱会频率太高可能竞争激烈）
                competition_penalty = max(0, (frequency - 10) * 0.5)  # 频率超过10场每月，每场扣0.5分
                score -= competition_penalty

                # 确保分数在0-100之间
                score = max(0, min(100, score))

                recommendations.append({
                    'city': city_name,
                    'score': round(score, 1),
                    'population': population,
                    'concert_frequency': frequency,
                    'avg_capacity': avg_capacity
                })

            # 按得分排序
            recommendations.sort(key=lambda x: x['score'], reverse=True)

            # 显示推荐结果
            st.subheader(f"{selected_singer}的城市推荐")

            for i, rec in enumerate(recommendations[:5], 1):  # 只显示前5个
                with st.container():
                    col1, col2, col3 = st.columns([1, 3, 1])
                    with col1:
                        st.markdown(f"### #{i}")
                    with col2:
                        st.markdown(f"#### 🏙️ {rec['city']}")

                        # 进度条颜色根据分数变化
                        if rec['score'] >= 80:
                            progress_color = "green"
                        elif rec['score'] >= 60:
                            progress_color = "blue"
                        else:
                            progress_color = "orange"

                        st.progress(rec['score'] / 100)
                    with col3:
                        st.markdown(f"**{rec['score']}分**")

                    # 显示详细信息
                    col_info1, col_info2, col_info3 = st.columns(3)
                    with col_info1:
                        st.metric("人口", f"{rec['population']}万")
                    with col_info2:
                        st.metric("月均演唱会", rec['concert_frequency'])
                    with col_info3:
                        st.metric("平均容量", f"{rec['avg_capacity']:,}")

                    # 推荐理由
                    if rec['score'] >= 80:
                        if '流行' in singer_genre and rec['city'] in ['北京', '上海']:
                            st.success("💡 推荐理由：一线城市对流行音乐接受度高，粉丝基础雄厚")
                        elif '摇滚' in singer_genre and rec['city'] in ['成都', '武汉']:
                            st.success("💡 推荐理由：新一线城市摇滚氛围浓厚，场地条件优越")
                        else:
                            st.success("💡 推荐理由：综合评分高，市场潜力大")
                    elif rec['score'] >= 60:
                        st.info("💡 推荐理由：市场条件良好，值得考虑")
                    else:
                        st.warning("💡 推荐理由：竞争较激烈或市场较小，需谨慎考虑")

                    st.markdown("---")


# def show_data_visualization():
#     """数据可视化页面"""
#     st.header("📈 数据可视化")
#
#     # 获取数据
#     singers_df = get_data('singers')
#     concerts_df = get_data('concerts')
#
#     if singers_df.empty or concerts_df.empty:
#         st.warning("暂无数据用于可视化，请先初始化数据库")
#         return
#
#     # 合并数据前确保数据类型正确
#     concerts_df = concerts_df.copy()
#     singers_df = singers_df.copy()
#
#     # 确保数值列是数值类型
#     numeric_cols_concerts = ['singer_id', 'capacity', 'attendance', 'ticket_price', 'revenue', 'attendance_rate']
#     for col in numeric_cols_concerts:
#         if col in concerts_df.columns:
#             concerts_df[col] = pd.to_numeric(concerts_df[col], errors='coerce')
#
#     if 'singer_id' in singers_df.columns:
#         singers_df['singer_id'] = pd.to_numeric(singers_df['singer_id'], errors='coerce')
#
#     # 合并数据 - 使用内连接确保数据匹配
#     merged_data = pd.merge(concerts_df, singers_df,
#                            left_on='singer_id', right_on='singer_id',
#                            how='inner')  # 改为内连接，确保只保留有对应关系的数据
#
#     if merged_data.empty:
#         st.warning("没有找到演唱会数据或数据不匹配")
#         return
#
#     # 清理数据：确保关键列没有NaN值
#     # 修复歌手姓名
#     if 'name' in merged_data.columns:
#         merged_data['name'] = merged_data['name'].fillna('未知歌手')
#         # 替换可能的错误字符
#         merged_data['name'] = merged_data['name'].replace({
#             'Taylor Swift': 'Taylor Swift',
#             '五月天': '五月天',
#             '周杰伦': '周杰伦',
#             '林俊杰': '林俊杰',
#             '邓紫棋': '邓紫棋'
#         })
#
#     # 修复收入数据
#     if 'revenue' in merged_data.columns:
#         merged_data['revenue'] = pd.to_numeric(merged_data['revenue'], errors='coerce')
#         merged_data['revenue'] = merged_data['revenue'].fillna(0)
#         # 移除负值
#         merged_data = merged_data[merged_data['revenue'] >= 0]
#
#     # 使用选项卡组织图表
#     tab1, tab2, tab3 = st.tabs(["📊 收入分析", "👥 上座率分析", "📍 城市分布"])
#
#     with tab1:
#         st.subheader("收入分析")
#
#         # 歌手收入排名 - 只显示有收入的歌手
#         if 'revenue' in merged_data.columns and 'name' in merged_data.columns:
#             # 按歌手分组计算总收入
#             singer_revenue = merged_data.groupby('name', as_index=False)['revenue'].sum()
#             singer_revenue = singer_revenue.sort_values('revenue', ascending=False).head(10)
#
#             if not singer_revenue.empty:
#                 # 确保数据有效
#                 singer_revenue = singer_revenue[singer_revenue['revenue'] > 0]
#
#                 fig1 = px.bar(
#                     singer_revenue,
#                     x='name',
#                     y='revenue',
#                     title="Top 10 歌手收入排名",
#                     color='revenue',
#                     color_continuous_scale='Viridis',
#                     labels={'revenue': '总收入 (元)', 'name': '歌手'}
#                 )
#
#                 # 格式化Y轴为货币格式
#                 fig1.update_layout(
#                     yaxis=dict(
#                         tickformat=",.0f",
#                         title="总收入 (元)"
#                     ),
#                     xaxis=dict(title="歌手"),
#                     height=500
#                 )
#
#                 st.plotly_chart(fig1, use_container_width=True)
#             else:
#                 st.info("暂无收入数据可展示")
#
#         # 收入分布
#         col1, col2 = st.columns(2)
#
#         with col1:
#             if 'revenue' in merged_data.columns and 'city' in merged_data.columns:
#                 # 按城市统计收入
#                 city_revenue = merged_data.groupby('city', as_index=False)['revenue'].sum()
#                 city_revenue = city_revenue[city_revenue['revenue'] > 0]
#
#                 if not city_revenue.empty:
#                     fig2 = px.pie(
#                         city_revenue,
#                         values='revenue',
#                         names='city',
#                         title="各城市收入占比",
#                         hole=0.4
#                     )
#                     st.plotly_chart(fig2, use_container_width=True)
#                 else:
#                     st.info("暂无城市收入数据")
#
#         with col2:
#             if 'revenue' in merged_data.columns and 'genre' in merged_data.columns:
#                 # 按音乐流派统计收入
#                 genre_revenue = merged_data.groupby('genre', as_index=False)['revenue'].sum()
#                 genre_revenue = genre_revenue[genre_revenue['revenue'] > 0]
#
#                 if not genre_revenue.empty:
#                     fig3 = px.pie(
#                         genre_revenue,
#                         values='revenue',
#                         names='genre',
#                         title="各音乐流派收入占比",
#                         hole=0.4
#                     )
#                     st.plotly_chart(fig3, use_container_width=True)
#                 else:
#                     st.info("暂无流派收入数据")
#
#     with tab2:
#         st.subheader("上座率分析")
#
#         if 'attendance_rate' in merged_data.columns:
#             # 计算平均上座率
#             merged_data['attendance_rate'] = pd.to_numeric(merged_data['attendance_rate'], errors='coerce')
#             merged_data['attendance_rate_percent'] = merged_data['attendance_rate'] * 100
#
#             # 只保留有效数据
#             valid_data = merged_data[merged_data['attendance_rate_percent'].notna()]
#
#             # 上座率最高的歌手
#             avg_rate_by_singer = valid_data.groupby('name', as_index=False)['attendance_rate_percent'].mean()
#             avg_rate_by_singer = avg_rate_by_singer.sort_values('attendance_rate_percent', ascending=False).head(10)
#
#             if not avg_rate_by_singer.empty:
#                 fig4 = px.bar(
#                     avg_rate_by_singer,
#                     x='name',
#                     y='attendance_rate_percent',
#                     title="Top 10 歌手平均上座率",
#                     color='attendance_rate_percent',
#                     color_continuous_scale='RdYlGn',
#                     labels={'attendance_rate_percent': '上座率 (%)', 'name': '歌手'}
#                 )
#
#                 fig4.update_layout(
#                     yaxis=dict(title="上座率 (%)"),
#                     xaxis=dict(title="歌手"),
#                     height=500
#                 )
#
#                 st.plotly_chart(fig4, use_container_width=True)
#             else:
#                 st.info("暂无上座率数据")
#
#     with tab3:
#         st.subheader("城市分布分析")
#
#         if 'city' in merged_data.columns:
#             # 各城市演唱会数量
#             city_concerts = merged_data.groupby('city').size().reset_index(name='count')
#             city_concerts = city_concerts.sort_values('count', ascending=False).head(10)
#
#             if not city_concerts.empty:
#                 fig5 = px.bar(
#                     city_concerts,
#                     x='city',
#                     y='count',
#                     title="各城市演唱会数量",
#                     color='count',
#                     color_continuous_scale='Plasma',
#                     labels={'count': '演唱会数量', 'city': '城市'}
#                 )
#
#                 fig5.update_layout(
#                     yaxis=dict(title="演唱会数量"),
#                     xaxis=dict(title="城市"),
#                     height=500
#                 )
#
#                 st.plotly_chart(fig5, use_container_width=True)
#             else:
#                 st.info("暂无城市分布数据")



def show_data_visualization():
    """数据可视化页面"""
    st.markdown("---")

    # 主标题
    st.header("📈 数据可视化")

    # 数据库连接状态
    if get_db_connection():
        st.success("✅ SQLite数据库已连接")
    else:
        st.error("❌ 数据库连接失败")

    # 获取数据
    try:
        singers_df = get_data('singers')
        concerts_df = get_data('concerts')
    except Exception as e:
        st.error(f"获取数据失败: {str(e)}")
        singers_df = pd.DataFrame()
        concerts_df = pd.DataFrame()

    # 实时统计卡片
    st.markdown("### 📊 实时统计")
    col1, col2, col3 = st.columns(3)

    with col1:
        singer_count = len(singers_df) if not singers_df.empty else 0
        st.metric("歌手总数", singer_count)

    with col2:
        concert_count = len(concerts_df) if not concerts_df.empty else 0
        st.metric("演唱会数量", concert_count)

    with col3:
        total_revenue = concerts_df[
            'revenue'].sum() if not concerts_df.empty and 'revenue' in concerts_df.columns else 0
        st.metric("总收入", f"¥{total_revenue:,.0f}")

    st.markdown("---")

    # 检查数据是否为空
    if singers_df.empty or concerts_df.empty:
        st.warning("暂无数据用于可视化，请先初始化数据库或添加数据")
        return

    # 数据清洗和合并
    try:
        # 确保有必要的列
        concerts_df = concerts_df.copy()
        singers_df = singers_df.copy()

        # 重命名列以便合并
        if 'singer_id' in singers_df.columns:
            singers_df = singers_df.rename(columns={'singer_id': 'singer_id'})
        if 'singer_id' in concerts_df.columns:
            concerts_df = concerts_df.rename(columns={'singer_id': 'singer_id'})

        # 确保singer_id是数值类型
        if 'singer_id' in singers_df.columns:
            singers_df['singer_id'] = pd.to_numeric(singers_df['singer_id'], errors='coerce')
        if 'singer_id' in concerts_df.columns:
            concerts_df['singer_id'] = pd.to_numeric(concerts_df['singer_id'], errors='coerce')

        # 确保其他数值列
        numeric_cols = ['capacity', 'attendance', 'ticket_price', 'revenue', 'attendance_rate']
        for col in numeric_cols:
            if col in concerts_df.columns:
                concerts_df[col] = pd.to_numeric(concerts_df[col], errors='coerce')

        # 合并数据 - 使用左连接，保留所有演唱会数据
        merged_data = pd.merge(
            concerts_df,
            singers_df[['singer_id', 'name', 'genre']].rename(columns={'name': 'singer_name'}),
            on='singer_id',
            how='left'
        )

        # 如果没有歌手名字，使用默认值
        if 'singer_name' in merged_data.columns:
            merged_data['singer_name'] = merged_data['singer_name'].fillna('未知歌手')
        else:
            merged_data['singer_name'] = '未知歌手'

        if 'genre' in merged_data.columns:
            merged_data['genre'] = merged_data['genre'].fillna('未知流派')
        else:
            merged_data['genre'] = '未知流派'

        # 填充其他缺失值
        if 'revenue' in merged_data.columns:
            merged_data['revenue'] = merged_data['revenue'].fillna(0)
        else:
            merged_data['revenue'] = 0

        if 'attendance_rate' in merged_data.columns:
            merged_data['attendance_rate'] = pd.to_numeric(merged_data['attendance_rate'], errors='coerce')
            merged_data['attendance_rate'] = merged_data['attendance_rate'].fillna(0)
        else:
            merged_data['attendance_rate'] = 0

        if 'city' in merged_data.columns:
            merged_data['city'] = merged_data['city'].fillna('未知城市')
        else:
            merged_data['city'] = '未知城市'

    except Exception as e:
        st.error(f"数据处理失败: {str(e)}")
        # 创建一个基本的合并数据用于显示
        merged_data = pd.DataFrame({
            'singer_name': ['周杰伦', '林俊杰', '邓紫棋', '五月天', 'Taylor Swift'],
            'revenue': [50000000, 30000000, 20000000, 15000000, 10000000],
            'city': ['北京', '上海', '广州', '深圳', '成都'],
            'genre': ['流行/R&B', '流行', '流行', '摇滚', '流行/乡村'],
            'attendance_rate': [0.95, 0.92, 0.88, 0.96, 0.94]
        })

    # 使用选项卡组织图表
    tab1, tab2, tab3 = st.tabs(["💰 收入分析", "👥 上座率分析", "📍 城市分布"])

    with tab1:
        st.subheader("💰 收入分析")

        # 1. 歌手收入排名
        if 'singer_name' in merged_data.columns and 'revenue' in merged_data.columns:
            try:
                singer_revenue = merged_data.groupby('singer_name')['revenue'].sum().reset_index()
                singer_revenue = singer_revenue.sort_values('revenue', ascending=False).head(10)

                if not singer_revenue.empty and singer_revenue['revenue'].sum() > 0:
                    # 创建柱状图
                    fig1 = px.bar(
                        singer_revenue,
                        x='singer_name',
                        y='revenue',
                        title="Top 10 歌手收入排名",
                        labels={'singer_name': '歌手', 'revenue': '总收入 (元)'},
                        color='revenue',
                        color_continuous_scale='Viridis'
                    )

                    # 优化布局
                    fig1.update_layout(
                        xaxis_title="歌手",
                        yaxis_title="总收入 (元)",
                        yaxis=dict(tickformat=",.0f"),
                        height=500,
                        showlegend=False
                    )

                    # 添加数据标签
                    fig1.update_traces(
                        texttemplate='%{y:,.0f}',
                        textposition='outside'
                    )

                    st.plotly_chart(fig1, use_container_width=True)
                else:
                    st.info("暂无收入数据可展示")
            except Exception as e:
                st.error(f"创建收入排名图失败: {str(e)}")

        # 2. 收入分布图
        st.markdown("#### 收入分布")
        col1, col2 = st.columns(2)

        with col1:
            # 城市收入占比
            if 'city' in merged_data.columns and 'revenue' in merged_data.columns:
                try:
                    city_revenue = merged_data.groupby('city')['revenue'].sum().reset_index()
                    city_revenue = city_revenue[city_revenue['revenue'] > 0]

                    if not city_revenue.empty:
                        fig2 = px.pie(
                            city_revenue,
                            values='revenue',
                            names='city',
                            title="各城市收入占比",
                            hole=0.3,
                            color_discrete_sequence=px.colors.sequential.RdBu
                        )
                        st.plotly_chart(fig2, use_container_width=True)
                    else:
                        st.info("暂无城市收入数据")
                except Exception as e:
                    st.error(f"创建城市收入图失败: {str(e)}")

        with col2:
            # 流派收入占比
            if 'genre' in merged_data.columns and 'revenue' in merged_data.columns:
                try:
                    genre_revenue = merged_data.groupby('genre')['revenue'].sum().reset_index()
                    genre_revenue = genre_revenue[genre_revenue['revenue'] > 0]

                    if not genre_revenue.empty:
                        fig3 = px.pie(
                            genre_revenue,
                            values='revenue',
                            names='genre',
                            title="各流派收入占比",
                            hole=0.3,
                            color_discrete_sequence=px.colors.sequential.Plasma
                        )
                        st.plotly_chart(fig3, use_container_width=True)
                    else:
                        st.info("暂无流派收入数据")
                except Exception as e:
                    st.error(f"创建流派收入图失败: {str(e)}")

    with tab2:
        st.subheader("👥 上座率分析")

        if 'attendance_rate' in merged_data.columns and 'singer_name' in merged_data.columns:
            try:
                # 计算平均上座率
                merged_data['attendance_rate_pct'] = merged_data['attendance_rate'] * 100

                # 按歌手分组计算平均上座率
                singer_attendance = merged_data.groupby('singer_name')['attendance_rate_pct'].mean().reset_index()
                singer_attendance = singer_attendance.sort_values('attendance_rate_pct', ascending=False).head(10)

                if not singer_attendance.empty:
                    fig4 = px.bar(
                        singer_attendance,
                        x='singer_name',
                        y='attendance_rate_pct',
                        title="Top 10 歌手平均上座率",
                        labels={'singer_name': '歌手', 'attendance_rate_pct': '上座率 (%)'},
                        color='attendance_rate_pct',
                        color_continuous_scale='RdYlGn'
                    )

                    fig4.update_layout(
                        xaxis_title="歌手",
                        yaxis_title="上座率 (%)",
                        height=500,
                        showlegend=False
                    )

                    # 添加数据标签
                    fig4.update_traces(
                        texttemplate='%{y:.1f}%',
                        textposition='outside'
                    )

                    st.plotly_chart(fig4, use_container_width=True)
                else:
                    st.info("暂无上座率数据")
            except Exception as e:
                st.error(f"创建上座率图失败: {str(e)}")
        else:
            st.info("暂无上座率数据")

    with tab3:
        st.subheader("📍 城市分布分析")

        if 'city' in merged_data.columns:
            try:
                # 各城市演唱会数量
                city_counts = merged_data['city'].value_counts().reset_index()
                city_counts.columns = ['city', 'count']
                city_counts = city_counts.sort_values('count', ascending=False).head(10)

                if not city_counts.empty:
                    fig5 = px.bar(
                        city_counts,
                        x='city',
                        y='count',
                        title="各城市演唱会数量",
                        labels={'city': '城市', 'count': '演唱会数量'},
                        color='count',
                        color_continuous_scale='Plasma'
                    )

                    fig5.update_layout(
                        xaxis_title="城市",
                        yaxis_title="演唱会数量",
                        height=500,
                        showlegend=False
                    )

                    # 添加数据标签
                    fig5.update_traces(
                        texttemplate='%{y}',
                        textposition='outside'
                    )

                    st.plotly_chart(fig5, use_container_width=True)
                else:
                    st.info("暂无城市分布数据")
            except Exception as e:
                st.error(f"创建城市分布图失败: {str(e)}")
        else:
            st.info("暂无城市分布数据")

    # 底部提示
    st.markdown("---")
    st.caption("💡 提示：图表数据基于数据库中的演唱会记录计算得出")


def show_database_management():
    """数据库管理页面"""
    st.header("📋 数据库管理")

    # 检查数据库连接
    conn = get_db_connection()

    if conn:
        tab1, tab2 = st.tabs(["🗃️ 表管理", "📊 数据统计"])

        with tab1:
            st.subheader("数据库表管理")

            # 获取所有表名
            tables_df = query_database("SELECT name FROM sqlite_master WHERE type='table'")

            if tables_df is not None and not tables_df.empty:
                selected_table = st.selectbox(
                    "选择要查看的表",
                    tables_df['name'].tolist()
                )

                if selected_table:
                    # 获取表数据
                    table_data = query_database(f"SELECT * FROM {selected_table} LIMIT 100")

                    if table_data is not None:
                        st.dataframe(table_data, use_container_width=True)

                        # 表信息
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("显示行数", len(table_data))
                        with col2:
                            st.metric("列数", len(table_data.columns))
                        with col3:
                            row_count = query_database(f"SELECT COUNT(*) as count FROM {selected_table}")
                            if row_count is not None and not row_count.empty:
                                st.metric("总行数", row_count['count'].iloc[0])

            # SQL查询工具
            st.subheader("SQL查询工具")
            sql_query = st.text_area(
                "输入SQL查询语句",
                height=100,
                value="SELECT * FROM singers LIMIT 10"
            )

            col1, col2 = st.columns(2)
            with col1:
                execute_btn = st.button("执行SQL", type="primary")
            with col2:
                reset_btn = st.button("重置", type="secondary")

            if execute_btn and sql_query.strip():
                try:
                    if sql_query.strip().upper().startswith('SELECT'):
                        result = query_database(sql_query)
                        if result is not None:
                            st.dataframe(result, use_container_width=True)
                    else:
                        # 执行非SELECT语句
                        success = execute_sql(sql_query)
                        if success:
                            st.success("SQL执行成功！")
                            # 刷新页面数据
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("SQL执行失败")
                except Exception as e:
                    st.error(f"执行错误: {str(e)}")

            if st.button("执行查询", type="primary"):
                if sql_query.strip().upper().startswith('SELECT'):
                    result = query_database(sql_query)
                    if result is not None:
                        st.dataframe(result, use_container_width=True)
                else:
                    st.warning("只支持SELECT查询")

        with tab2:
            st.subheader("数据统计")

            # 获取各表数据量
            tables = ['singers', 'concerts', 'popularity', 'cities']
            stats = []

            for table in tables:
                count_df = query_database(f"SELECT COUNT(*) as count FROM {table}")
                if count_df is not None and not count_df.empty:
                    stats.append({
                        '表名': table,
                        '记录数': count_df['count'].iloc[0]
                    })

            if stats:
                stats_df = pd.DataFrame(stats)
                st.dataframe(stats_df, use_container_width=True)

                # 可视化
                fig = px.bar(
                    stats_df,
                    x='表名',
                    y='记录数',
                    title="各表数据量统计",
                    color='记录数',
                    color_continuous_scale='Viridis'
                )
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("数据库连接不可用，无法进行数据库管理操作")


def show_system_settings():
    """系统设置页面"""
    st.header("⚙️ 系统设置")

    tab1, tab2 = st.tabs(["🔧 系统配置", "ℹ️ 关于"])

    with tab1:
        st.subheader("系统配置")

        # 数据设置
        st.selectbox("默认页面", ["系统概览", "歌手管理", "演唱会管理", "热度分析", "数据可视化"])

        # 可视化设置
        col1, col2 = st.columns(2)
        with col1:
            st.selectbox("图表主题", ["Plotly", "Matplotlib"])
        with col2:
            st.selectbox("颜色主题", ["明亮", "暗黑", "自动"])

        # 数据缓存
        cache_time = st.slider("数据缓存时间(秒)", 60, 3600, 600)
        st.info(f"当前缓存时间: {cache_time}秒")

        if st.button("保存设置", type="primary"):
            st.success("设置已保存")

    with tab2:
        st.subheader("关于系统")

        st.markdown("""
        ### 🎵 演唱会管理信息系统

        **版本**: 1.0.0
        **最后更新**: 2025年12月

        ### 功能特点

        1. **歌手管理**: 完整的歌手信息管理
        2. **演唱会管理**: 记录和管理所有演唱会数据
        3. **热度分析**: 实时追踪歌手热度和粉丝增长
        4. **预测分析**: 基于历史数据预测未来趋势
        5. **数据可视化**: 丰富的图表展示数据分析结果
        6. **城市推荐**: 智能推荐最佳演唱会举办城市

        ### 技术栈

        - **后端**: Python + SQLite
        - **前端**: Streamlit
        - **数据可视化**: Plotly, Matplotlib
        - **预测模型**: Scikit-learn

        ### 开发团队

        本系统由演唱会管理团队开发，旨在为投资方提供数据驱动的决策支持。
        """)


# ==================== 主程序 ====================

# 页面标题
st.title("🎵 星筹——演唱会管理信息系统")
st.markdown("面向投资方的商业价值分析平台")

# 检查数据库是否存在
db_exists = os.path.exists("concert_management.db")

if not db_exists:
    st.warning("⚠️ 数据库文件不存在，正在初始化数据库...")

    # 显示初始化进度
    progress_text = st.empty()
    progress_text.text("正在初始化数据库...")

    # 尝试自动初始化数据库
    try:
        # 直接调用初始化函数
        if initialize_database():
            progress_text.text("✅ 数据库初始化成功！")
            st.success("数据库初始化成功，正在重新加载页面...")

            # 清除缓存
            st.cache_data.clear()

            # 等待并重新加载
            import time

            time.sleep(2)
            st.rerun()
        else:
            st.error("❌ 数据库初始化失败！")
            st.info("请尝试以下方法：")
            st.code("""
            在本地运行：
            python init_database.py

            或在云环境中手动创建数据库文件
            """)
            st.stop()
    except Exception as e:
        st.error(f"❌ 数据库初始化过程中出现错误：{str(e)}")
        st.stop()
else:
    # 数据库已存在，检查表结构是否完整
    try:
        # 简单的表检查
        conn = get_db_connection()
        if conn:
            tables_to_check = ['singers', 'concerts', 'popularity', 'cities']
            missing_tables = []

            for table in tables_to_check:
                result = query_database(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
                if result is None or result.empty:
                    missing_tables.append(table)

            if missing_tables:
                st.warning(f"⚠️ 缺少表：{', '.join(missing_tables)}，正在修复...")
                init_database()  # 只初始化表结构
                st.success("✅ 表结构修复完成！")
                st.rerun()
    except Exception as e:
        st.warning(f"⚠️ 数据库检查失败：{str(e)}")
        # 继续运行，尝试连接

conn = get_db_connection()
if conn is None:
    st.error("❌ 无法连接到数据库！")

    # 尝试修复
    if st.button("🔧 尝试修复数据库连接"):
        try:
            # 重新初始化数据库
            initialize_database()
            st.success("✅ 数据库修复完成，正在重新加载...")
            st.rerun()
        except Exception as e:
            st.error(f"修复失败：{str(e)}")
    st.stop()

# 侧边栏导航
st.sidebar.title("导航菜单")

# 直接显示所有菜单选项
menu_options = [
    "🏠 系统概览",
    "🎤 歌手管理",
    "🎫 演唱会管理",
    "📊 热度分析",
    "🏙️ 城市管理",
    "🔮 预测分析",
    "📈 数据可视化",
    "📋 数据库管理",
    "⚙️ 系统设置"
]

page = st.sidebar.radio("选择功能", menu_options)

# 侧边栏统计信息
st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 实时统计")

try:
    singer_count = query_database("SELECT COUNT(*) as count FROM singers")
    concert_count = query_database("SELECT COUNT(*) as count FROM concerts")

    if singer_count is not None and not singer_count.empty:
        st.sidebar.metric("歌手数量", singer_count['count'].iloc[0])
    else:
        st.sidebar.metric("歌手数量", "0")

    if concert_count is not None and not concert_count.empty:
        st.sidebar.metric("演唱会数量", concert_count['count'].iloc[0])
    else:
        st.sidebar.metric("演唱会数量", "0")

except Exception as e:
    st.sidebar.metric("歌手数量", "0")
    st.sidebar.metric("演唱会数量", "0")

# 数据库连接状态
if conn:
    st.sidebar.success("✅ SQLite数据库已连接")
else:
    st.sidebar.warning("❌ 数据库连接失败")

# 页面路由
if page == "🏠 系统概览":
    show_system_overview()
elif page == "🎤 歌手管理":
    show_singer_management()
elif page == "🎫 演唱会管理":
    show_concert_management()
elif page == "📊 热度分析":
    show_popularity_analysis()
elif page == "🏙️ 城市管理":
    show_city_management()
elif page == "🔮 预测分析":
    show_prediction_analysis()
elif page == "📈 数据可视化":
    show_data_visualization()
elif page == "📋 数据库管理":
    show_database_management()
elif page == "⚙️ 系统设置":
    show_system_settings()

# 页脚
st.markdown("---")
st.caption("星筹——演唱会管理信息系统 © 2025 | 为投资决策提供数据支持")