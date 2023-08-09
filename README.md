# Kafka-docker-example

Descarga Kafdrop

```
wget https://github.com/obsidiandynamics/kafdrop/releases/download/3.31.0/kafdrop-3.31.0.jar
```

Descarga Kafka y descomprime el tgz
``` 
tar -xzf kafka-3.5.0-src.tgz
```

Inicia zookeper
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Inicia Kafka
```
bin/kafka-server-start.sh config/server.properties
```

Si kafka no inicia, dentro de la carpeta de kafka, ejecuta el siguiente comando y después el comando de arriba
```
./gradlew jar -PscalaVersion=2.13.10
```

Inicia kafdrop 
```
java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -jar kafdrop-3.31.0.jar --kafka.brokerConnect=localhost:9092
```

