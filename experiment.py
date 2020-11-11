#encoding:utf-8

from exact_weight import ExactWeightSampling
from extended_olken import ExtendedOlkenSampling
from online_exploration import OnlineExplorationSampling

import matplotlib.pyplot as plt
import time


def social_graph_full_dataset_compare(query):
    a = OnlineExplorationSampling("twitter_combined.db", None, None)
    b = ExtendedOlkenSampling("twitter_combined.db", None, None)
    c = ExactWeightSampling("twitter_combined.db", None, None)
    random_nums = [100, 1000]
    times = [[], [], []]
    p = []
    for i, random_num in enumerate(random_nums):
        t1 = time.process_time()
        wt = a.sampling(random_num, query, 15, 200000, 0.9)
        t2 = time.process_time()
        if i == 0:
            times[0].append(t2 - t1 - wt)
        else:
            times[0].append(t2 - t1)
        print(times)
        t1 = time.process_time()
        wt = b.sampling(random_num, query, 500, "popular_frequency.txt", "twitter_frequency.txt")
        t2 = time.process_time()
        if i == 0:
            times[1].append(t2 - t1 - wt)
        else:
            times[1].append(t2 - t1)
        print(times)
        t1 = time.process_time()
        wt = c.sampling(random_num, query)
        t2 = time.process_time()
        if i == 0:
            times[2].append(t2 - t1 - wt)
        else:
            times[2].append(t2 - t1)
        print(times)
    labels = ["Online Exploration", "Extended Olken", "Exact Weight"]
    '''for i in range(3):
        plt.plot(random_nums, times[i], 'x-', label=labels[i])
    plt.xlabel("Number of samples collected")
    plt.ylabel("Time(seconds)")
    plt.legend()
    #plt.savefig('./S.jpg')
    plt.show()'''


if __name__ == "__main__":
    #social_graph_full_dataset_compare(["Popular_user", "Twitter_user", "Twitter_user"])
    '''[[1040.453125, 2706.046875, 21589.703125], [478.4375, 3680.59375], [185.390625, 1895.578125]]'''
    k = [i for i in range(0, 1000, 50)]
    plt.plot(k, [24604.0, 12709.0, 10068.0, 9127.0, 8979.0, 8973.0, 8909.0, 8909.0, 8904.0, 8900.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0, 8862.0][0:20], 'x-', label='uniform_origin')#691
    plt.plot(k, [24219.0, 13138.0, 10199.0, 9542.0, 8739.0, 8586.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0, 8498.0][0:20], 'x-', label='random_origin')
    plt.plot(k, [23710.0, 11438.0, 9495.0, 8526.0, 8403.0, 8224.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0, 8148.0][0:20], 'x-', label='min_tree')
    plt.xlabel("Iteration")
    plt.ylabel("Best Result")
    plt.legend()
    plt.show()



