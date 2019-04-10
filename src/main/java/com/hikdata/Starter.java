package com.hikdata;

import org.apache.hadoop.util.RunJar;

/**
 * 应用启动入口，运行应用时将data目录下的数据文件拷贝到input目录
 * input目录数据文件和应用对应关系
 * 数据去重DataClean：data-clean.txt data-clean1.txt
 * 经典单词计数WordCount:README.txt
 * 简单数据排序DataSort：sort.txt
 * 数据二次排序：secondary-sort.txt、secondary-sort2.txt
 * 倒排索引：README.txt
 * 二度好友关系发现CommonFriends：relation.txt
 */
public class Starter {
    public static void main(String[] args) throws Throwable {
        /**
         * 二度好友发现应用
         * RunJar接收参数
         * [0]: 必选，要执行的jar位置
         * [1]: 必选，要执行的程序主类
         * [...] 可选，其他参数供主类解析
         */
        String[] parmas = new String[]{
                "E:/project/mapreduce-test/mapreduce-test.jar",
                "com.hikdata.CommonFriends",
                "input/*",
                "output/"
        };
        RunJar.main(parmas);
    }
}
