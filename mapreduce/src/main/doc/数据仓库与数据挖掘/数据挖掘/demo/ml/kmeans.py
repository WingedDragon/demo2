# coding=utf-8

import random
from sklearn.cluster import KMeans
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.externals import joblib
from sklearn import metrics

#参数
gl_kmeans_init = 'k-means++'
gl_kmeans_n_init = 10
gl_kmeans_max_iter = 10
gl_kmeans_tol=1e-4
gl_kmeans_precompute_distances='auto'
gl_kmeans_verbose=0
gl_kmeans_random_state=None
gl_kmeans_copy_x =True
gl_kmeans_n_jobs=1

#数据参数
gl_height_big = 200
gl_height_small = 150
gl_height_step = 3

gl_weight_big = 100
gl_weight_small = 50
gl_weight_step = 5

gl_examples_count = 100

#数据文件
gl_data_in_path = "datas/test.csv"
gl_data_out_path = "datas/test-kmeans.csv"


####################################################


#获取数据
def get_test_data_fn(count=gl_examples_count):
    # 返回范围内的一个随机数
    heights_range = range(gl_height_small, gl_height_big, gl_height_step)
    weights_range = range(gl_weight_small, gl_weight_big, gl_weight_step)
    data = list()
    for idx in range(count):
        height = random.choice(heights_range)
        weight= random.choice(weights_range)
        person = [height, weight]
        print("idx[{2}]: person={3},height={0},weight={1}".format(height, weight, idx, person))
        data.append(person)

    return np.array(data,np.int32)



#测试数据
def get_data_fn(inpath=gl_data_in_path):



    return  0




################
 #kmeans聚类
 # data 数据
 # n_clusters 簇数量k
 # init 初始值选择的方式 : {‘k-means++’, ‘random’, or ndarray, or a callable}
 # n_init 获取初始簇中心的更迭次数
 # max_iter 最大迭代次数
 # tol: 容忍度，即kmeans运行准则收敛的条件
 # precompute_distances：是否需要提前计算距离，这个参数会在空间和时间之间做权衡，
#       如果是True 会把整个距离矩阵都放到内存中，auto 会默认在数据样本大于featurs*samples 的数量大于12e6 的时候False,
#      False 时核心实现的方法是利用Cpython 来实现的
################
def kmeans_fn(data, n_clusters, init=gl_kmeans_init, n_init=gl_kmeans_n_init, max_iter=gl_kmeans_max_iter,
                 tol=gl_kmeans_tol, precompute_distances=gl_kmeans_precompute_distances,
                 verbose=gl_kmeans_verbose, random_state=gl_kmeans_random_state,
                 copy_x=gl_kmeans_copy_x, n_jobs=gl_kmeans_n_jobs):
    #模型构建和训练
    kmeans = KMeans(n_clusters=n_clusters, init=gl_kmeans_init, n_init=gl_kmeans_n_init, max_iter=gl_kmeans_max_iter,
                 tol=gl_kmeans_tol, precompute_distances=gl_kmeans_precompute_distances,
                 verbose=gl_kmeans_verbose, random_state=gl_kmeans_random_state,
                 copy_x=gl_kmeans_copy_x, n_jobs=gl_kmeans_n_jobs)
    kmeans.fit(data)

    #模型结果
    label_pred = kmeans.labels_  # 获取聚类标签
    centroids = kmeans.cluster_centers_  # 获取聚类中心

    print("label_pred={0}".format(label_pred))
    print("centroids={0}".format(centroids))

    #结果
    data_result = kmeans.fit_predict(data)
    print("data_result={0}".format(data_result))

    #结果绘图
    plt.scatter(data[:, 0], data[:, 1], c=data_result)
    plt.show()

    #Calinski-Harabasz Index评估的聚类分数
    result_metrics = metrics.silhouette_score(data, label_pred)
    print("result_metrics={0}".format(result_metrics))


    # 注释语句用来存储你的模型
    #oblib.dump(km, 'doc_cluster.pkl')
    #km = joblib.load('doc_cluster.pkl')
    #clusters = km.labels_.tolist()



if __name__ == '__main__':

    # 数据
    count = 30
    persons = get_test_data_fn(count)
    print(persons)
    print(type(persons))

    # 聚类运算
    clusters = 5
    kmeans_fn(persons, n_clusters=clusters)