# kafka-cat [![Build Status](https://img.shields.io/badge/license-MIT-blue.svg)](http://opensource.org/licenses/MIT)

实现部分 kafka 协议, 快速定位 broker 是否可以读写。 fetch, produce 以及 获取 metadata 的耗时

### install

```
$ cd kafka-cat/src
$ sudo make && make install
```

### usage
```
Usage: ./kafka-cat
    -b broker list, like localhost:9092.
    -t topic name.
    -c client id.
    -C consumer mode.
    -p partition id.
    -P producer mode.
    -o consumer offset.
    -f consumer fetch size.
    -k produce message key.
    -v produce message value.
    -l loglevel debug, info, warn, error .
    -h help.
```

### offset example

```
$ kafka-cat -b 127.0.0.1:9092 -t test_topic -O
```

### metadata fetch example 

```
$ kafka-cat -b 127.0.0.1:9092 -t test_topic
```

### consume example

```
$ kafka-cat -b 127.0.0.1:9092 -t test_topic -o 100 -C
```

### produce example

```
 $ kafka-cat -b 127.0.0.1:9092 -t test_topic -k test_key -v test_value -P
```
