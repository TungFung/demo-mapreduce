##Windows环境下运行前置条件
1.将winutils.exe和hadoop.dll放到$HADOOP_HOME/bin目录下;
2.将hadoop.dll放到C:\windows\System32目录下；
3.如果遇到ExitCode的问题，安装Windows C++运行环境集合;

##分布式Hadoop集群下运行
mvn package生成运行的jar包
把工程目录下的wordCount.txt文件和demo-mapreduce.jar上传到linux服务器hadoop用户目录下

在HDFS中创建目录
hadoop fs -mkdir /user
hadoop fs -mkdir /user/hadoop
hadoop fs -mkdir /user/hadoop/output

wordCount.txt文件需要上传到HDFS /user/hadoop/ 目录下
hadoop fs -put ./wordCount.txt /user/hadoop/

运行MR任务
hadoop jar demo-mapreduce.jar /user/hadoop/wordCount.txt /user/hadoop/output/