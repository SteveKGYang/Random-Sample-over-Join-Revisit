from sampling import Sampling
import copy
import time


class ExtendedOlkenSampling(Sampling):
    def __init__(self, db_file, popular_user_file, twitter_user_file):
        super(ExtendedOlkenSampling, self).__init__(db_file, popular_user_file, twitter_user_file)
        self.W = None

    def get_frequency(self, popular_save_file, twitter_save_file):
        pu_tuple = {}
        tu_tuple = {}
        tf = open(twitter_save_file, "w+", encoding="utf8")
        with open(popular_save_file, "w+", encoding="utf8") as f:
            pu_frequency = self.conn.execute("SELECT source, SUM(count) FROM Popular_user GROUP BY source")
            tu_frequency = self.conn.execute("SELECT source, SUM(count) FROM Twitter_user GROUP BY source")
            for result in pu_frequency:
                pu_tuple[result[0]] = result[1]
                f.write(str(result[0])+" "+str(result[1])+"\n")
            for result in tu_frequency:
                tu_tuple[result[0]] = result[1]
                tf.write(str(result[0]) + " " + str(result[1])+"\n")
        return pu_tuple, tu_tuple

    def load_frequency(self, popular_save_file, twitter_save_file):
        pu_tuple = {}
        tu_tuple = {}
        with open(popular_save_file, "r", encoding="utf8") as f:
            for line in f:
                a, b = line.strip().split()
                pu_tuple[int(a)] = int(b)
        with open(twitter_save_file, "r", encoding="utf8") as f:
            for line in f:
                a, b = line.strip().split()
                tu_tuple[int(a)] = int(b)
        return pu_tuple, tu_tuple

    def AGM_bound(self, lengths):
        #bound = lengths[0]*lengths[len(lengths)-1]
        bound = lengths[len(lengths) - 1]
        min_num = float("inf")
        if len(lengths) > 2:
            for i in range(1, len(lengths)-1):
                if min_num > lengths[i]:
                    min_num = lengths[i]
            bound *= min_num
        return bound

    def olken_bound(self, frequency):
        bound = 1.
        for fre in frequency:
            bound *= fre
        return bound

    def combined_method(self, table_names, pu_frequencies,
                                 pu_table_lengths, tu_frequencies, tu_table_lengths):
        W = 0
        for i in range(2**len(table_names)):
            #length = [first_length]
            #frequency = [first_length]
            length = []
            frequency = []
            k = str(bin(i))[2:]
            if len(k) < len(table_names):
                for i in range(len(table_names)-len(k)):
                    k = str(0) + k
            for j, table_name in enumerate(table_names):
                if table_name == "Twitter_user":
                    length.append(tu_table_lengths[int(k[j])])
                    frequency.append(tu_frequencies[int(k[j])])
                else:
                    length.append(pu_table_lengths[int(k[j])])
                    frequency.append(pu_frequencies[int(k[j])])

            ob = self.olken_bound(frequency)
            agmb = self.AGM_bound(length)
            if ob > agmb:
                W += agmb
            else:
                W += ob
        return W

    def extended_olken(self, h, popular_save_file, twitter_save_file, join_order):
        '''self.conn.execute("CREATE INDEX Popular_user_index ON Popular_user(source)")
        self.conn.execute("CREATE INDEX Twitter_user_index ON Twitter_user(source)")'''
        print("开始计算W值...")
        W = []
        pu_tuple, tu_tuple = self.load_frequency(popular_save_file, twitter_save_file)
        max_pu_frequency, max_tu_frequency = max(pu_tuple.values()), max(tu_tuple.values())
        tu_heavy_length = 0
        pu_heavy_length = 0
        tu_light_length = 0
        pu_light_length = 0
        tu_light_frequency = h
        pu_light_frequency = h
        for k, t in tu_tuple.items():
            if t < h:
                '''if t > tu_light_frequency:
                    tu_light_frequency = t'''
                fr = self.conn.execute("SELECT COUNT(*) FROM Twitter_user WHERE source={}".format(k))
                for r in fr:
                    tu_light_length += r[0]
            else:
                fr = self.conn.execute("SELECT COUNT(*) FROM Twitter_user WHERE source={}".format(k))
                for r in fr:
                    tu_heavy_length += r[0]
        for k, t in pu_tuple.items():
            if t < h:
                '''if t > pu_light_frequency:
                    pu_light_frequency = t'''
                fr = self.conn.execute("SELECT COUNT(*) FROM Popular_user WHERE source={}".format(k))
                for r in fr:
                    pu_light_length += r[0]
            else:
                fr = self.conn.execute("SELECT COUNT(*) FROM Popular_user WHERE source={}".format(k))
                for r in fr:
                    pu_heavy_length += r[0]
        pu_frequencies = [max_pu_frequency, pu_light_frequency]
        tu_frequencies = [max_tu_frequency, tu_light_frequency]
        pu_table_lengths = [pu_heavy_length, pu_light_length]
        tu_table_lengths = [tu_heavy_length, tu_light_length]
        fr = self.conn.execute("SELECT COUNT(*) FROM " + join_order[0])
        first_length = None
        for result in fr:
            first_length = result[0]
        other_order = join_order[1:len(join_order)]
        '''w = self.combined_method(first_length, other_order, pu_frequencies,
                                 pu_table_lengths, tu_frequencies, tu_table_lengths)'''
        w = self.combined_method(other_order, pu_frequencies,
                                 pu_table_lengths, tu_frequencies, tu_table_lengths)
        W.append({self.r0: w*first_length})
        for i in range(len(join_order)-2):
            W_set = {}
            ts = self.conn.execute("SELECT * FROM " + join_order[i])
            other_order = join_order[i + 2:len(join_order)]
            w = self.combined_method(other_order, pu_frequencies,
                                     pu_table_lengths, tu_frequencies, tu_table_lengths)
            for j, result in enumerate(ts):
                fr = self.conn.execute(
                        "SELECT COUNT(*) FROM " + join_order[i + 1] + " WHERE source=" + str(result[1]))
                for r in fr:
                    first_length = r[0]
                '''other_order = join_order[i + 2:len(join_order)]
                w = self.combined_method(first_length, other_order, pu_frequencies,
                                         pu_table_lengths, tu_frequencies, tu_table_lengths)'''
                W_set[(result[0], result[1])] = w*first_length
            W.append(W_set)
        ts = self.conn.execute("SELECT * FROM " + join_order[len(join_order)-2])
        W_set = {}
        for result in ts:
            fr = self.conn.execute("SELECT COUNT(*) FROM " + join_order[len(join_order)-1]
                                   + " WHERE source=" + str(result[1]))
            for r in fr:
                first_length = r[0]
            W_set[(result[0], result[1])] = first_length
        W.append(W_set)

        ts = self.conn.execute("SELECT * FROM " + join_order[len(join_order)-1])
        W_set = {}
        for result in ts:
            W_set[(result[0], result[1])] = 1
        W.append(W_set)
        self.W = W
        return W

    def sampling(self, sample_num, join_order, h, popular_save_file, twitter_save_file):
        t1 = 0
        t2 = 0
        if self.W is None:
            t1 = time.process_time()
            W = self.extended_olken(h, popular_save_file, twitter_save_file,
                                    join_order)
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
    a = ExtendedOlkenSampling("twitter_combined.db", "popular_frequency.txt", "twitter_frequency.txt")
    s = a.sampling(10, ["Popular_user", "Twitter_user", "Twitter_user"], 600, "popular_frequency.txt", "twitter_frequency.txt")
    print(s)