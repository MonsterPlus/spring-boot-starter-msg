#使用方式

##一、依赖maven坐标
```xml
<dependency>
    <groupId>com.huake.msg</groupId>
    <artifactId>spring-boot-starter-msg</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
``` 

##二、在application文件中添加相关配置
###示例：
```properties
spring.boot.kafka.enabled=true
spring.boot.kafka.cfg.channels[0].channelId=test
spring.boot.kafka.cfg.channels[0].consumerChannel.topics=test,test_01,test_02
spring.boot.kafka.cfg.channels[0].consumerChannel.groupId=test
spring.boot.kafka.cfg.channels[0].consumerChannel.bootstrapServers=10.182.200.81:9092
spring.boot.kafka.cfg.channels[0].producerChannel.topic=test
spring.boot.kafka.cfg.channels[0].producerChannel.bootstrapServers=10.182.200.81:9092
spring.boot.kafka.cfg.channels[0].producerChannel.async=true
spring.boot.kafka.cfg.channels[0].producerChannel.msgFailPath=D:/msgFailPath
```
详细参见

 [KafkaConsumerProperties](https://github.com/MonsterPlus/spring-boot-starter-msg/blob/master/src/main/java/com/huake/msg/kafka/conf/KafkaConsumerProperties.java)
 
 [KafkaProducerProperties](https://github.com/MonsterPlus/spring-boot-starter-msg/blob/master/src/main/java/com/huake/msg/kafka/conf/KafkaProducerProperties.java)
 
 ##三、在应用中使用
 
 请参看KafkaUtils中提供的api