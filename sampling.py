#encoding:utf-8
import sqlite3
import numpy as np
import os

ALLJOIN = -1
threshold = 0.5


class Sampling:
    def __init__(self, db_file, popular_user_file, twitter_user_file):
        self.r0 = (ALLJOIN, ALLJOIN)
        if os.path.exists(db_file):
            self.conn = sqlite3.connect(db_file)
            self.cursor = self.conn.cursor()
        else:
            self.conn = sqlite3.connect(db_file)
            self.cursor = self.conn.cursor()
            self.create_database(popular_user_file, twitter_user_file)

    def create_database(self, popular_user_file, twitter_user_file):
        self.conn.execute("CREATE TABLE Popular_user (source, destination, count)")
        self.conn.execute("CREATE TABLE Twitter_user (source, destination, count)")
        self.conn.commit()
        self.conn.execute('PRAGMA synchronous = OFF')
        print("开始插入popular数据...")
        with open(popular_user_file, "r", encoding="utf8") as f:
            datas = []
            for num, line in enumerate(f):
                if num % 1000000 == 0:
                    print("popular数据已插入{}条".format(num))
                    s, d = line.strip().split()
                    datas.append((int(s), int(d), 1))
                    self.conn.executemany("INSERT INTO Popular_user"
                                          "(source, destination, count) VALUES(?,?,?)", datas)
                    datas.clear()
                else:
                    s, d = line.strip().split()
                    datas.append((int(s), int(d), 1))
        if len(datas) > 0:
            self.conn.executemany("INSERT INTO Popular_user"
                                  "(source, destination, count) VALUES(?,?,?)", datas)
        self.conn.commit()

        print("开始插入twitter数据...")
        with open(twitter_user_file, "r", encoding="utf8") as f:
            datas = []
            for num, line in enumerate(f):
                if num % 1000000 == 0:
                    print("Twitter数据已插入{}条".format(num))
                    s, d = line.strip().split()
                    datas.append((int(s), int(d), 1))
                    self.conn.executemany("INSERT INTO Twitter_user"
                                          "(source, destination, count) VALUES(?,?,?)", datas)
                    datas.clear()
                else:
                    s, d = line.strip().split()
                    datas.append((int(s), int(d), 1))
        if len(datas) > 0:
            self.conn.executemany("INSERT INTO Twitter_user"
                                  "(source, destination, count) VALUES(?,?,?)", datas)
        self.conn.commit()

    def chain_join_sampling(self, join_order, W):
        t = self.r0
        S = {self.r0}
        for i in range(len(join_order)):
            wt = W[i][t]
            if i == 0:
                p = "select " + join_order[i] + ".source, " + join_order[i] + ".destination" + " from " + join_order[i]
                tRi = self.conn.execute(p)
                tRI = self.conn.execute(p)
                WtRi = 0
                for result in tRi:
                    WtRi += W[i+1][result]
            else:
                p = "select "+join_order[i]+".source, "+join_order[i]+".destination"+" from "+join_order[i]+\
                    " where " + str(t[1]) + "=" + join_order[i]+".source"
                tRi = self.conn.execute(p)
                tRI = self.conn.execute(p)
                WtRi = 0
                for result in tRi:
                    WtRi += W[i+1][result]

            W[i][t] = WtRi
            reject_prob = 1 - WtRi/wt
            '''print(i)
            print(reject_prob)
            print(WtRi)
            print(wt)'''
            if np.random.rand() <= min(reject_prob, threshold):
                return None
            num = np.random.rand()
            p = 0.
            sample = None
            for result in tRI:
                p += W[i+1][result]/WtRi
                if num < p:
                    sample = result
                    t = result
                    break
            if sample is not None:
                S.add(sample)
        return S


if __name__ == "__main__":
    s = Sampling("twitter_combined.db", "popular_user_table.txt", "twitter_combined.txt")
    #s.chain_join_sampling(["Popular_user", "Twitter_user"], 1)
