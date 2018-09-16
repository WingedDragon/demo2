# -*- coding:utf-8 -*-

import pandas as pd
import datetime as dt


#####

gl_csv_sep=','
gl_csv_header=0

gl_utf8_encoding='utf-8'
gl_gbk_encoding='gbk'



################
# 时间转化
################
def parse_dates(x):
    return dt.datetime.strptime(x, '%Y%m%d %H:%M:%S')

################
 # csv数据读取
 # sep 分隔(delim_whitespace=True 空白分隔 覆盖sep)
 # header 指定标题行位置(覆盖names)
 # index_col 行索引(int/sequence/none)
 # dtype 列数据类型 np.int32 | {'colname':np.int32}
################
def read_chunk_csv(inpath, u_sep, u_header, u_names,
             u_usecols, u_chunksize, u_encoding):

    return pd.read_csv(inpath, sep=u_sep, header=u_header, names=u_names,
                usecols=u_usecols, chunksize=u_chunksize,
                encoding=u_encoding)


#普通方式(小数据)
def read_csv(inpath, u_sep, u_header, u_names, u_usecols, u_encoding=gl_utf8_encoding):

    return pd.read_csv(inpath, sep=u_sep, header=u_header, names=u_names,
                usecols=u_usecols, encoding=u_encoding)



#写入csv
def write_csv(df, outpath, u_sep, u_columns, u_header, u_index_label,
              u_mode, u_encoding, u_chunksize, u_date_format, u_decimal):

    df.to_csv(outpath, sep=u_sep, columns=u_columns, header=u_header,
              mode=u_mode, encoding=u_encoding, date_format=u_date_format)




if __name__ == '__main__':

    inpath = "datas/test.csv"
    u_sep = gl_csv_sep
    u_header = gl_csv_header
    u_names = ["user_id","sign_platform","app_id","app_name","geo_ip","app_pv","data_date","stat_date"]
    u_usecols = ["user_id","sign_platform","app_id","app_name","geo_ip","app_pv","data_date","stat_date"]
    u_encoding = gl_utf8_encoding
    u_dtypes = None


    #数据集
    user_df = read_csv(inpath, u_sep, u_header, u_names, u_usecols, u_encoding)

    sample_df = user_df.head(5)
    print("sample_df={0}".format(sample_df))


    #转置
    print("*" * 100)
    transposing_df = sample_df.T
    print("transposing_df={0}".format(transposing_df))

