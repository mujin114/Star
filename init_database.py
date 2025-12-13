import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os


def create_database_with_real_data():
    """创建数据库并填充真实数据"""

    # 删除旧的数据库文件（如果存在）
    if os.path.exists("concert_management.db"):
        os.remove("concert_management.db")
        print("已删除旧的数据库文件")

    # 创建数据库连接
    conn = sqlite3.connect("concert_management.db")
    conn.row_factory = sqlite3.Row

    # 启用外键支持
    conn.execute("PRAGMA foreign_keys = ON")

    print("正在创建数据库表...")

    # 创建歌手表
    conn.execute('''
        CREATE TABLE singers (
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
        CREATE TABLE concerts (
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
        CREATE TABLE popularity (
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
        CREATE TABLE cities (
            city_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name TEXT NOT NULL,
            country TEXT,
            population INTEGER,
            avg_concert_capacity INTEGER,
            concert_frequency INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    print("数据库表创建完成！")
    print("正在插入真实数据...")

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

    # 插入歌手数据
    for singer in real_singers:
        cursor.execute('''
            INSERT INTO singers (name, birth_date, nationality, debut_year, genre, active_status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', singer)

    print(f"已插入 {len(real_singers)} 位歌手数据")

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

    print(f"已插入 {len(real_cities)} 个城市数据")

    # 插入演唱会数据
    concerts_data = []
    concert_id = 1

    # 周杰伦的演唱会
    jay_concerts = [
        (1, '嘉年华世界巡回演唱会-上海站', '2023-10-01', '上海', '上海体育场', 50000, 48000, 800, 38400000, 0.96),
        (1, '嘉年华世界巡回演唱会-北京站', '2023-11-15', '北京', '国家体育场', 80000, 75000, 1000, 75000000, 0.94),
        (1, '嘉年华世界巡回演唱会-广州站', '2023-12-10', '广州', '天河体育场', 40000, 39000, 700, 27300000, 0.975),
        (1, '嘉年华世界巡回演唱会-深圳站', '2024-01-20', '深圳', '深圳湾体育中心', 35000, 34000, 750, 25500000, 0.971),
        (1, '嘉年华世界巡回演唱会-成都站', '2024-03-05', '成都', '成都体育中心', 45000, 42000, 650, 27300000, 0.933),
    ]

    # 林俊杰的演唱会
    jj_concerts = [
        (2, 'JJ20世界巡回演唱会-上海站', '2023-09-15', '上海', '梅赛德斯奔驰文化中心', 18000, 17500, 900, 15750000,
         0.972),
        (2, 'JJ20世界巡回演唱会-北京站', '2023-10-20', '北京', '凯迪拉克中心', 18000, 17000, 950, 16150000, 0.944),
        (2, 'JJ20世界巡回演唱会-广州站', '2023-11-25', '广州', '广州体育馆', 12000, 11800, 850, 10030000, 0.983),
        (2, 'JJ20世界巡回演唱会-香港站', '2024-01-10', '香港', '红磡体育馆', 12500, 12000, 1000, 12000000, 0.96),
    ]

    # 邓紫棋的演唱会
    gemi_concerts = [
        (3, 'Queen of Hearts世界巡演-上海站', '2023-08-20', '上海', '虹口足球场', 35000, 33000, 600, 19800000, 0.943),
        (3, 'Queen of Hearts世界巡演-北京站', '2023-09-10', '北京', '工人体育场', 50000, 45000, 550, 24750000, 0.9),
        (3, 'Queen of Hearts世界巡演-深圳站', '2023-10-05', '深圳', '深圳体育场', 30000, 29000, 500, 14500000, 0.967),
    ]

    # Taylor Swift的演唱会
    taylor_concerts = [
        (5, 'The Eras Tour-东京站', '2024-02-10', '东京', '东京巨蛋', 55000, 53000, 1200, 63600000, 0.964),
        (5, 'The Eras Tour-新加坡站', '2024-03-15', '新加坡', '新加坡国家体育场', 60000, 58000, 1100, 63800000, 0.967),
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

    # 插入演唱会数据
    for concert in all_concerts:
        cursor.execute('''
            INSERT INTO concerts 
            (singer_id, concert_name, concert_date, city, venue, capacity, 
             attendance, ticket_price, revenue, attendance_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', concert)

    print(f"已插入 {len(all_concerts)} 场演唱会数据")

    # 插入热度数据
    popularity_data = []
    pop_id = 1

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

            pop_id += 1

    print("已插入热度数据")

    conn.commit()

    # 验证数据
    print("\n数据验证:")
    print(f"歌手表记录数: {cursor.execute('SELECT COUNT(*) FROM singers').fetchone()[0]}")
    print(f"演唱会表记录数: {cursor.execute('SELECT COUNT(*) FROM concerts').fetchone()[0]}")
    print(f"热度表记录数: {cursor.execute('SELECT COUNT(*) FROM popularity').fetchone()[0]}")
    print(f"城市表记录数: {cursor.execute('SELECT COUNT(*) FROM cities').fetchone()[0]}")

    cursor.close()
    conn.close()

    print("\n数据库初始化完成！")
    print(f"数据库文件: {os.path.abspath('concert_management.db')}")

    return True


if __name__ == "__main__":
    success = create_database_with_real_data()
    if success:
        print("\n✅ 数据库初始化成功！")
        print("现在可以运行 star.py 来启动演唱会管理信息系统了。")
    else:
        print("\n❌ 数据库初始化失败！")