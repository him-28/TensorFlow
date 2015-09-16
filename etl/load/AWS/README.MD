# 一、简介

## Amazon S3
Amazon Simple Storage Service (Amazon S3) 为开发人员和 IT 团队提供安全、耐久且扩展性高的对象存储。Amazon S3 易于使用，具有简单的 Web 服务接口，用于在 Web 上的任何位置存储和检索任意数量的数据。使用 Amazon S3，您只需按您实际使用的存储量付费。没有最低费用和准备成本。
Amazon S3 可单独使用，或与 Amazon Elastic Compute Cloud (Amazon EC2)、Amazon Elastic Block Store (Amazon EBS) 和 Amazon Glacier 等 AWS 服务以及第三方存储库和网关结合使用。Amazon S3 为各种各样的使用案例提供低本高效的对象存储服务，其中包括云应用程序、内容分发、备份和归档、灾难恢复以及大数据分析。

> 更多关于Amazon S3的[入门指南](http://docs.aws.amazon.com/AmazonS3/latest/gsg/)、[开发人员指南](http://docs.aws.amazon.com/AmazonS3/latest/dev/)、[API参考](http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)、[控制台用户指南](http://docs.aws.amazon.com/AmazonS3/latest/UG/)、[快速参考卡片](http://awsdocs.s3.amazonaws.com/S3/latest/s3-qrc.pdf)的信息，可在[Amazon Simple Storage Service](https://aws.amazon.com/cn/documentation/s3/?icmpid=docs_menu "Amazon Simple Storage Service 文档")文档中获取

## Amazon EMR
Amazon Elastic MapReduce (Amazon EMR) 是一种 Web 服务，让您能够轻松快速并经济地处理大量的数据。

Amazon EMR 简化了大数据处理，提供的托管 Hadoop 框架可以让您轻松、快速、经济高效地跨越各个动态可扩展的 Amazon EC2 实例分发和处理巨量数据。您还可以运行其他常用的分发框架（例如 Amazon EMR 中的 Spark 和 Presto）与其他 AWS 数据存储服务（例如 Amazon S3 和 Amazon DynamoDB）中的数据进行互动。

Amazon EMR 能够安全可靠地处理大数据使用案例，包括日志分析、Web 索引、数据仓库、机器学习、财务分析、科学模拟和生物信息。

## 如何使用 Amazon EMR
要使用 Amazon EMR，您只需：

* 1. **开发数据处理应用。**您可以使用 Java、Hive（类似 SQL 语言）、Pig（数据处理语言）、Cascading、Ruby、Perl、Python、R、PHP、C++ 或者 Node.js。 Amazon EMR 提供代码示例和教程，帮助您快速开始使用并正常运行。
* 2. **上传您的应用和数据到 Amazon S3。**如果您拥有大量上传数据，可以考虑使用 AWS Import/Export（使用物理存储设备上传数据）或者 AWS Direct Connect（建立从数据中心到 AWS 的专用网络连接）。 如果您愿意，还可以直接向正在运行的集群写入数据。
* 3. **配置和启动您的集群。**使用AWS 管理控制台、AWS CLI、SDK或API，指定要在集群中预配置的 EC2 实例数、要使用的实例种类（标准、内存增强型、CPU 增强型、高 I/O 等等）、要安装的应用程序（Hive、Pig、HBase 等等）以及应用程序和数据的位置。您可以使用引导操作安装其他软件或者更改默认设置。
* 4. **可选）监控集群。**您可以使用管理控制台、命令行界面、软件开发工具包或者 API 监控集群的运行状况和进度。 EMR 与 Amazon CloudWatch 集成，可用于监控/警报，并支持流行的监控工具 Ganglia。 您可以随时根据数据的处理情况给集群添加/移除容量。 对于故障诊断，您可以使用控制台的简易调试 GUI。
* 5. **检索输出。**检索集群上的 Amazon S3 或者 HDFS 中的输出。 使用工具（如 Tableau 和 MicroStrategy）直观显示数据。 Amazon EMR 会在处理完成时自动终止集群。 另一种方法是，让集群处于运行状态并给群集增加工作量。

> 更多关于Amazon EMR的[开发人员指南](http://docs.aws.amazon.com/zh_cn/ElasticMapReduce/latest/DeveloperGuide/emr-what-is-emr.html)、[API参考](http://docs.aws.amazon.com/ElasticMapReduce/latest/API/)、[快速参考卡片](http://s3.amazonaws.com/awsdocs/ElasticMapReduce/latest/emr-qrc.pdf)的信息，可在[Amazon Elastic MapReduce](http://aws.amazon.com/cn/documentation/elasticmapreduce/ "Amazon Elastic MapReduce 文档")文档中获取

# 二、准备工作
## 获取适用于 Python 的 AWS 开发工具包 (Boto3)

通过适用于 Python 的 AWS 开发工具包 boto3 快速开始使用 AWS。Boto3 可以支持您轻松将 Python 应用程序、库或脚本与 AWS 服务进行集成，包括 Amazon S3、Amazon EC2 和 Amazon DynamoDB 等。

Boto3安装： pip install boto3
> 附： pip安装：下载 [get-pip.py](https://bootstrap.pypa.io/get-pip.py "get-pip.py").
> 
> 运行`python get-pip.py`， 更多信息参见[https://pip.pypa.io/en/stable/installing/](https://pip.pypa.io/en/stable/installing/ "pip安装官方说明")

更多关于Python的AWS开发包的[入门](https://boto3.readthedocs.org/en/latest/guide/quickstart.html)、[API 参考](https://boto3.readthedocs.org/en/latest/reference/services/index.html)、[社区论坛](https://forums.aws.amazon.com/forum.jspa?forumID=132)信息参见：[http://aws.amazon.com/cn/sdk-for-python/](http://aws.amazon.com/cn/sdk-for-python/ "Amazon Web Services SDK for Python")


## 获取适用于 Java 的 AWS 开发工具包

通过适用于 Java 的 AWS 开发工具包迅速开始使用 AWS。该开发工具包提供 Java API 给许多 AWS 服务，如 Amazon S3、Amazon EC2、DynamoDB 等，以避免进行复杂的编码。这个可下载的软件包含有 AWS Java 库、代码示例和文档。 


### AWS Toolkit for Eclipse
在Eclipse中在线安装地址[http://aws.amazon.com/cn/eclipse](http://aws.amazon.com/cn/eclipse "eclipse aws 插件")插件
> 安装工具包
> 
> * 1. 打开“Help”->“Install New Software…”
> * 2. 在对话框顶部标有“Work with”的文本框中，输入 http://aws.amazon.com/cn/eclipse。
> * 3. 从下方列表中选择 “AWS Toolkit for Eclipse”。
> * 4. 单击“Next”Eclipse 将引导您完成剩余的安装步骤。
> 
> 请注意：该工具包需要 Eclipse 3.6 或更高的版本。

更多关于适用于Java的AWS开发工具包的[入门](http://aws.amazon.com/developers/getting-started/java/)、[开发人员指南](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/welcome.html)、[API文档](http://docs.amazonwebservices.com/AWSJavaSDK/latest/javadoc/index.html)、[开发人员博客](http://java.awsblog.com/)信息可在这里找到：[http://aws.amazon.com/cn/sdk-for-java/](http://aws.amazon.com/cn/sdk-for-java/ "Amazon Web Services SDK for Java")

## 配置Access Key
使用AWS开发工具包，需要先配置连接AWS的Access Key ID和Secret Access Key

### 获取Access Key
访问 [IAM Management Console](https://console.aws.amazon.com/iam/home#home)，在左则的 **控制面板** 中选择 **用户** ，选择一个用户名点击，如果没有用户名，点击 **创建新用户** 创建一个。在 **安全证书** 版块中，点击 **Create Access Key** 按钮，生成后记录下生成的 `Access Key ID` 和 `Secret Access Key` 备用

### 配置Access Key
> * Windows用户可直接在`C:\Users\Administrator\.aws\`目录下（替换`Administrator`成当前用户）、Linux用户在` ~/.aws/`目录下新建文件**credentials**，文件内容为：

	[default]  
	aws_access_key_id = YOUR ACCESS KEY ID 
	aws_secret_access_key = YOUR SECRET ACCESS KEY

> * 如果安装了AWS 命令行界面，可以运行aws命令来配置

	aws configure

> * 更多关于Python配置Access Key的信息参见[https://boto3.readthedocs.org/en/latest/guide/configuration.html#guide-configuration](https://boto3.readthedocs.org/en/latest/guide/configuration.html#guide-configuration)

**Notice:** *开发工具包提供在程序代码中配置credentials的方法，详细点击本文提供的API文档或开发人员指获取*

配置完成Access Key后，即可使用AWS开发工具包提供的API访问AWS许多服务

# 三、功能规划

	┌─────────────────────┐        ┌───────────────────┐        ┌───────────────────────┐
	│上传数据文件到S3 Input│  --->  │spark定时处理数据   │  --->  │输出数据文件到S3 Output  │  
	└─────────────────────┘        └───────────────────┘        └───────────────────────┘

# 四、文件上传到S3 API

功能说明：上传文件到Amazon S3，测试上传速度约160KB/s,有断点上传功能；Git地址：[http://git.hunantv.com/dingzheng/amble/tree/dev/etl/load/AWS/scripts/awss3.py](http://git.hunantv.com/dingzheng/amble/tree/dev/etl/load/AWS/scripts/awss3.py)。

存储桶：Amazon S3每个用户只能使用自己的Access Key操作自己创建的存储桶，现阶段暂定名称为`cn-north-region-java`的存储桶

# 五、spark处理数据

功能说明：处理S3 Input的数据；Git地址[http://git.hunantv.com/dingzheng/amble/tree/dev/etl/load/AWS/scripts/spark.py](http://git.hunantv.com/dingzheng/amble/tree/dev/etl/load/AWS/scripts/spark.py)。