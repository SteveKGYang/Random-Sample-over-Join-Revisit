from sampling import Sampling
import copy
import time


class ExactWeightSampling(Sampling):
    def __init__(self, db_file, popular_user_file, twitter_user_file):
        super(ExactWeightSampling, self).__init__(db_file, popular_user_file, twitter_user_file)
        self.W = None

    def exact_weight(self, join_order):
        print("开始计算W值...")
        W = []
        for i in range(len(join_order)-1, -1, -1):
            W_set = {}
            if i == len(join_order)-1:
                ts = self.conn.execute("SELECT source, destination FROM " + join_order[i])
                for result in ts:
                    W_set[result] = 1
                W.append(W_set)
            else:
                next_set = W[0]
                ts = self.conn.execute("SELECT source, destination FROM " + join_order[i])
                ds = self.conn.execute("SELECT DISTINCT destination FROM " + join_order[i])
                s_set = {}
                for result in ds:
                    p = "select " + join_order[i + 1] + ".source, " + join_order[i + 1] + ".destination" + " from " + \
                        join_order[i + 1] + \
                        " where " + str(result[0]) + "=" + join_order[i + 1] + ".source"
                    path_tuples = self.conn.execute(p)
                    m = 0
                    for t in path_tuples:
                        m += next_set[t]
                    s_set[result[0]] = m
                for result in ts:
                    W_set[result] = s_set[result[1]]
                W = [W_set] + W
        m = sum(W[0].values())
        W = [{self.r0: m}] + W
        self.W = W
        return W

    def sampling(self, sample_num, join_order):
        t1 = 0
        t2 = 0
        if self.W is None:
            t1 = time.process_time()
            W = self.exact_weight(join_order)
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
    a = ExactWeightSampling("twitter_combined.db", "popular_frequency.txt", "twitter_frequency.txt")
    '''ds = a.conn.execute("SELECT DISTINCT destination FROM Twitter_user")
    p = 0
    for d in ds:
        p+=1
    print(p)'''
    s = a.sampling(10, ["Popular_user", "Twitter_user", "Twitter_user"])
    print(s)
