# kafka-cat [![Build Status](https://img.shields.io/badge/license-MIT-blue.svg)](http://opensource.org/licenses/MIT)

handy tool to inspect kafka info

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
    -T offsets timestamp, -1 = LATEST, -2 = EARLIEST.
    -c client id.
    -C consumer mode.
    -p partition id.
    -P producer mode.
    -o consumer offset.
    -O fetch offsets.
    -L show topic list.
    -f consumer fetch size.
    -k produce message key.
    -v produce message value.
    -l loglevel debug, info, warn, error .
    -h help.
```

### topic list example

```
$ kafka-cat -b 127.0.0.1:9092 -L
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
