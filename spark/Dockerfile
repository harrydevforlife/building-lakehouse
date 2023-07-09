FROM docker.io/bitnami/spark:3.3

USER root

# Install prerequisites
RUN apt-get update && apt-get install -y curl

RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar \
    && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && mv s3-2.18.41.jar /opt/bitnami/spark/jars \
    && mv aws-java-sdk-1.12.367.jar /opt/bitnami/spark/jars \
    && mv delta-core_2.12-2.2.0.jar /opt/bitnami/spark/jars \
    && mv delta-storage-2.2.0.jar /opt/bitnami/spark/jars \
    && mv mysql-connector-java-8.0.19.jar /opt/bitnami/spark/jars \
    && mv hadoop-aws-3.3.4.jar /opt/bitnami/spark/jars