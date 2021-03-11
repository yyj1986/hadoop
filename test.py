from hdfs.client import Client
import os, tarfile

client = Client("http://10.239.1.57:50070")

#list()会列出hdfs指定路径的所有文件信息,接收两个参数
print("hdfs中的目录为:", client.list(hdfs_path="/test1",status=True))


#read()
#读取文件信息 类似与 hdfs dfs -cat hfds_path,参数如下:
# hdfs_path hdfs路径
# offset 读取位置
# length 读取长度
# buffer_size 设置buffer_size 不设置使用hdfs默认100MB 对于大文件 buffer够大的化 sort与shuffle都更快
# encoding 指定编码
# chunk_size 字节的生成器,必须和encodeing一起使用 满足chunk_size设置即 yield
# delimiter 设置分隔符 必须和encodeing一起设置
# progress 读取进度回调函数 读取一个chunk_size回调一次


print(client.read("/test1/part-00000-15b6e708-1025-408b-a6f2-1f37a7fe7064-c000.csv"))

# 下载
print("下载文件结果part.csv:", client.download(hdfs_path="/test1/part-00000-15b6e708-1025-408b-a6f2-1f37a7fe7064-c000.csv", local_path="/home/yyj2020/test",overwrite=True))

# 打包
with tarfile.open("/home/yyj2020/test/tartest.tar.gz", "w:gz") as tar:
    tar.add("/home/yyj2020/test/files", arcname=os.path.basename("/home/yyj2020/test/files"))
    tar.close()