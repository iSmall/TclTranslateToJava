# 打包前需更改以下配置文件:
### vars/prod.properties
 - jdbc_url --> 正确的数据库url
 - jdbc_user --> 正确的数据库用户名
 - jdbc_password --> 正确的数据库密码
 - register_center --> 正确的注册中心地址,redis格式:redis://192.168.0.106:6379;zookeeper格式:zookeeper://127.0.0.1:2181
 - SCHEMA_FT --> 正确的schema名称	
 - SCHEMA_DW --> 正确的schema名称	
 - dbType --> 正确的数据库类型

### src/main/resources/config/log/prod_log_config.xml  
 修改此xml中的日志文件存放地址,以及日志级别  
 其中<logger name="druid.sql" additivity="false">这个logger配置不要改,否则不能打印sql(目前程序里面所有执行的sql都会打印)
		
### 如果数据库需要改成gbase,需要将prod.properties中的dbType改成gbase,然后在src/main/resources/config/mapper目录下新建gbase文件夹,将db2下的xml文件拷贝到gbase文件夹下,并将xml中的db2的sql语法转换成gbase的sql语法

### db2的jar包需要本地引入,修改pom.xml中db2的dependency的<systemPath>${project.basedir}/lib/db2jcc-9.7.jar</systemPath>为自己电脑的jar包位置

### 打包命令:
mvn clean; mvn compile; mvn -Pprod package

### 执行命令:
java -jar /opt/java/tag/tag-0.0.1.jar



