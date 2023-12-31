# bigdata4
本次实验我选择在windows系统下安装运行spark
IDE使用IntelliJ IDEA，在其中安装scala组件并导入spark包
![image](https://github.com/Aristellaaa/bigdata4/assets/127272945/fb7a71d8-6df6-490e-a315-dee93b2f52e2)
任务一
1.统计application_data.csv中所有⽤户的贷款⾦额AMT_CREDIT 的分布情况。以 10000 元为区间进⾏输出。
要点在于如何将差为10000以内的数据合并。
这里使用除以10000取整的思路，这样就可以将相近的数合并。
使用reduceByKey将数据合并后输出，代码如下：
![image](https://github.com/Aristellaaa/bigdata4/assets/127272945/ed5ada23-2307-4cf2-b407-d33bec263d10)
（注：在读取csv时应过滤掉第一行）
输出见CreditCount out文件夹，这里贴出一部分：
![image](https://github.com/Aristellaaa/bigdata4/assets/127272945/a9052bdd-b5bb-4337-8486-c126cead052f)

2.统计application_data.csv中客户贷款⾦额AMT_CREDIT ⽐客户收⼊AMT_INCOME_TOTAL差值最⾼和最低的各⼗条记录。
首先使用map将csv文件中不需要的列过滤掉，只剩下需要的列；在此过程中顺带计算出差值，作为键值对的value。
接着使用sortBy以value进行排序，再用take提取升序和降序各前十条数据
代码如下：
![image](https://github.com/Aristellaaa/bigdata4/assets/127272945/aa92addd-8286-47ef-8d7e-14ade6f7b633)
输出结果见CreditIncome.txt:
![image](https://github.com/Aristellaaa/bigdata4/assets/127272945/99861a4b-4733-44c6-a824-f238d2459ace)

任务二
1.统计所有男性客户（CODE_GENDER=M）的⼩孩个数（CNT_CHILDREN）类型占⽐情况。
先使用filter过滤出男性客户的条目，再用reduceByKey统计相同小孩个数的人数；
利用count获取男性客户总数，进而用mapValues计算出比例。
（注：在mapValues中不能直接引用count，会导致报错）
![image](https://github.com/Aristellaaa/bigdata4/assets/127272945/5a14e51a-676d-454e-8ef3-e0eb2de7889b)
输出结果见Children out，截取一部分如下：
![image](https://github.com/Aristellaaa/bigdata4/assets/127272945/16d81afe-e076-4469-a9d5-26f434256acf)

2.统计每个客户出⽣以来每天的平均收⼊（avg_income）=总收⼊（AMT_INCOME_TOTAL）/出⽣天数（DAYS_BIRTH)，统计每⽇收⼊⼤于1的客户，并按照从⼤到⼩排序，保存为csv。
先使用filter过滤出平均收入大于1的条目，直接使用map整合数据并用sortBy排序
最后使用spark.createDataFrame将数据转为DataFrame并导出到csv，代码如下：
![image](https://github.com/Aristellaaa/bigdata4/assets/127272945/d56a5380-0467-4c10-9547-e040e6de98ea)
输出结果见AvgIncome.csv，部分结果如下：
![image](https://github.com/Aristellaaa/bigdata4/assets/127272945/f361dddf-77f0-40d5-b8da-f249116a4e73)

