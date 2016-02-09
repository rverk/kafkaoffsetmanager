.. title:: Home

==================
KafkaOffsetManager
==================

**Kafka Offsert Manager** is a simple pluggable offset manager for
use in Spark streaming jobs.

Spark streaming is great for interacting with Kafka, but leaves offsetmanagement
up to the user. Sparks' HDFS snapshots and using zookeeper are far from ideal.
Kafka 0.9 provides a better way to store offsets, but I wanted to be fully in control
and preferred a separate reliable system to store offsets.

This project provides a simple API(kom-core) that gives access to the earliest, stored
and latest offsets of a kafka topic given a consumer/group ID. It's backend is flexible,
all you need to do is implement an Interface(IOffsetDao) for storing and retrieving offsets.

The kom-dao package contains a Dao implementation for an HBase 1.0 backend.

Dependencies:
=============

This project has two modules. The core, which provides the offsetmanager and
a dao implementation of a hbase1.0 backend.

To just use the core with your own dao implementation:

.. code-block:: xml

    <dependency>
        <groupId>org.ctrl-r</groupId>
        <artifactId>kom-core</artifactId>
        <version>1.0.0</version>
    </dependency>


If you also want to use the dao implementation:

.. code-block:: xml

    <dependency>
        <groupId>org.ctrl-r</groupId>
        <artifactId>kom-dao</artifactId>
        <version>1.0.0</version>
    </dependency>


Example
=======

The example below illustrates basic usage of the library.

.. code-block:: java

    //...spark streaming jobsetup

    // Setup the DAO Implementation
    final IOffsetStore dao = new Hbase1OffsetStore.Builder()
        .setHbaseConfiguration(hbaseConfiguration)
        .setOffsetTable("offsettable").build();

    // Setup the offsetmanager
    final KafkaOffsetManager osm = new KafkaOffsetManager.Builder()
        .setOffsetManager(dao)
        .setKafkaBrokerList("localhost:9092")
        .setGroupID("someGroupID")
        .setTopic("someTopic").build();

    //...spark job and offsets


See example code in kom-examples.


Core documentation
==================

    .. toctree::
        :maxdepth: 2

        manual/index
        about/index

