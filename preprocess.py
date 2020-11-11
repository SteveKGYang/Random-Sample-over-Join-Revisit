#encoding:utf-8
import snap
import numpy as np


def get_attribute(origin_file):
    min_num = float("inf")
    max_num = float("-inf")
    records_num = 0
    with open(origin_file, "r", encoding="utf8") as f:
        for line in f:
            if records_num % 1000 == 0:
                print("已处理{}行".format(records_num))
            records_num += 1
            source, des = line.strip().split()
            source = int(source)
            des = int(des)
            max_n = max(source, des)
            min_n = min(source, des)
            if max_n > max_num:
                max_num = max_n
            if min_n < min_num:
                min_num = min_n
    print("记录数{}，最大值{}，最小值{}".format(records_num, max_num, min_num))


def divide_table(origin_file, save_file):
    print("开始构建图...")
    f = open(save_file, "w+", encoding="utf8")
    graph = snap.LoadEdgeList(snap.PNGraph, origin_file, 0, 1)
    print("图构建完成。开始计算名人...")
    for node in graph.Nodes():
        if node.GetInDeg() > 100:
            for item in node.GetOutEdges():
                f.write(str(node.GetId())+" "+str(item)+"\n")
            for item in node.GetInEdges():
                f.write(str(item)+" "+str(node.GetId())+"\n")
    f.close()
    print("计算完成。")


if __name__ == "__main__":
    divide_table("twitter_combined.txt", "popular_user_table.txt")