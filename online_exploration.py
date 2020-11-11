from sampling import Sampling
import random
from scipy.stats import norm
import copy
import time


class OnlineExplorationSampling(Sampling):
    def __init__(self, db_file, popular_user_file, twitter_user_file):
        super(OnlineExplorationSampling, self).__init__(db_file, popular_user_file, twitter_user_file)
        self.W = None

    def random_walk(self, random_walk_time, join_order):
        walk_time = [{self.r0: random_walk_time}]
        walks = []
        records = {}
        rt = self.conn.execute("SELECT source, destination FROM Twitter_user")
        rt_tuples = []
        for result in rt:
            rt_tuples.append(result)
        rt_len = len(rt_tuples)
        records["Twitter_user"] = rt_tuples
        rp = self.conn.execute("SELECT source, destination FROM Popular_user")
        rp_tuples = []
        for result in rp:
            rp_tuples.append(result)
        rp_len = len(rp_tuples)
        records["Popular_user"] = rp_tuples
        for order in join_order:
            time = {}
            for record in records[order]:
                time[record] = 0
            walk_time.append(time)
        print("开始进行random walk...")
        for i in range(random_walk_time):
            if i % 10000 == 0:
                print("已进行{}次random walk.".format(i))
            walk = [self.r0]
            u = []
            for j in range(len(join_order)):
                if j == 0:
                    sample = random.choice(records[join_order[j]])
                    walk_time[j+1][sample] += 1
                    walk.append(sample)
                    if join_order[j] == "Twitter_user":
                        u.append(rt_len)
                    else:
                        u.append(rp_len)
                else:
                    p = "select " + join_order[j] + ".source, " + join_order[j] + ".destination" + " from " + \
                        join_order[j] + " where " + str(walk[-1][1]) + "=" + join_order[j] + ".source"
                    tt = self.conn.execute(p)
                    s = []
                    for t in tt:
                        s.append(t)
                    if len(s) == 0:
                        u.append(0)
                        break
                    sample = random.choice(s)
                    walk_time[j+1][sample] += 1
                    walk.append(sample)
                    u.append(len(s))
            walks.append([walk, u])
        return walk_time, walks, records

    def wander_join_estimator(self, walks, record, i, alpha, ):
        Y = 0.
        n = 0
        sigma = 0.
        ls = []
        for walk, u in walks:
            if len(walk) < i+2:
                continue
            if record == walk[i+1]:
                k = 1.
                for j in range(i+1, len(u)):
                    k *= u[j]
                Y += k
                n += 1
                ls.append(k)
        Y /= n
        for k in ls:
            sigma += (k-Y)**2
        sigma /= (n-1)

        ipsalon = sigma**0.5 * norm.ppf((alpha+1)/2, loc=0, scale=1) / n**0.5
        return Y+ipsalon

    def dynamic_programming(self, W, record, join_order, i):
        p = "select " + join_order[i+1] + ".source, " + join_order[i+1] + ".destination" + " from " + \
            join_order[i+1] + \
            " where " + str(record[1]) + "=" + join_order[i+1] + ".source"
        tt = self.conn.execute(p)
        w = 0
        for result in tt:
            w += W[0][result]
        return w

    def online_exploration(self, threshold, join_order, random_walk_time, alpha):
        walk_time, walks, whole_records = self.random_walk(random_walk_time, join_order)
        W = []
        W_set = {}
        for result in whole_records[join_order[-1]]:
            W_set[result] = 1
        W.append(W_set)

        for i in range(len(join_order)-2, -1, -1):
            W_set = {}
            wander_set = []
            pro_list = []
            pro_set = set({})
            for result in whole_records[join_order[i]]:
                if walk_time[i+1][result] > threshold:
                    wander_set.append(result)
                else:
                    pro_list.append(result)
                    pro_set.add((-1, result[1]))
            set_num = {}
            for result in wander_set:
                W_set[result] = self.wander_join_estimator(walks, result, i, alpha)
            for result in pro_set:
                set_num[result] = self.dynamic_programming(W, result, join_order, i)
            for result in pro_list:
                W_set[result] = set_num[(-1, result[1])]
            W = [W_set] + W
        W = [{self.r0: self.wander_join_estimator(walks, self.r0, -1, alpha)}] + W
        self.W = W
        return W

    def sampling(self, sample_num, join_order, threshold, random_walk_time, alpha):
        t1 = 0
        t2 = 0
        if self.W is None:
            t1 = time.process_time()
            W = self.online_exploration(threshold, join_order, random_walk_time, alpha)
            t2 = time.process_time()
        else:
            W = copy.deepcopy(self.W)
        samples = set({})
        print("开始进行sample的计算...")
        i = 0
        while i < sample_num:
            S = self.chain_join_sampling(join_order, W)
            if S is not None:
                i += 1
        return t2-t1
        #return samples


if __name__ == "__main__":
    a = OnlineExplorationSampling("twitter_combined.db", "popular_frequency.txt", "twitter_frequency.txt")
    #w, wa, records = a.random_walk(100000, ["Popular_user", "Twitter_user", "Twitter_user"])
    #b = a.online_exploration(200, ["Popular_user", "Twitter_user", "Twitter_user"], 200000, 0.9)
    print(a.sampling(10, ["Popular_user", "Twitter_user", "Twitter_user"], 50, 200, 0.9))
    print(a.sampling(10, ["Popular_user", "Twitter_user", "Twitter_user"], 50, 200, 0.9))