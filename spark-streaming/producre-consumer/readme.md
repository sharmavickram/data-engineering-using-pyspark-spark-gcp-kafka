How to Install Apache Kafka on Windows/wsl?

Kafka is not intended to be run on Windows natively and has several issues that may arise over time.

Therefore, it is recommended to run Apache Kafka on Windows through:

    If using Windows 10 or above: WSL2 (see below) or Docker
    If using Windows 8 or below: Docker
    It is not recommended to run Kafka on the JVM on Windows, because it lacks some of the linux-specific features of POSIX for example. 
        You will run into issues at some point if you try to run Kafka on Windows without WSL2. (examples: KAFKA-8811 and KAFKA-1194)

How to install Kafka with Zookeeper on Windows
    You must have Windows 10 or above
    
    1. Install WSL2
    2. Install Java JDK 
    3. Download Apache Kafka from https://kafka.apache.org/downloads under Binary
    4. Downloads
    5. Extract the contents on WSL2
    6. Start Zookeeper using the binaries in WSL2
    7. Start Kafka using the binaries in another process in WSL2
    8. Setup the $PATH environment variables for easy access to the Kafka binaries

Install Apache Kafka

1. Download the latest version of Apache Kafka from https://kafka.apache.org/downloads under Binary downloads.

    The download page for Apache Kafka where you can download and install Kafka.
2. Click on any of the binary downloads (it is preferred to choose the most recent Scala version - example 2.13). For this illustration, we will assume version 2.13-3.0.0.

Alternatively you can run a wget command
    wget https://archive.apache.org/dist/kafka/3.0.0/kafka_2.13-3.0.0.tgz

3. Download and extract the contents to a directory of your choice, for example ~/kafka_2.13-3.0.0 .
    tar xzf kafka_2.13-3.0.0.tgz
    mv kafka_2.13-3.0.0 ~

4. Open a Shell and navigate to the root directory of Apache Kafka. For this example, we will assume that the Kafka download is expanded into the ~/kafka_2.13-3.0.0 directory.

Start Zookeeper
Apache Kafka depends on Zookeeper for cluster management. Hence, prior to starting Kafka, Zookeeper has to be started. There is no need to explicitly install Zookeeper, as it comes included with Apache Kafka.

Make sure your JAVA_HOME environment variable is set first, as instructed in the Install Java section, so that Java 11 is used when doing java -version

From the root of Apache Kafka, run the following command to start Zookeeper:
    dataedge@VIKRAMSHARMA:~/hadoop/kafka_2.13-3.0.0$ ./bin/zookeeper-server-start.sh /home/dataedge/hadoop/kafka_2.13-3.0.0/config/zookeeper.properties
                ![img.png](img.png)    

Start Apache Kafka
Open another Shell window and run the following command from the root of Apache Kafka to start Apache Kafka.
