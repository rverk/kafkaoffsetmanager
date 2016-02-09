.. _manual-index:

###########
User Manual
###########

DAO
---

To build your own DAO, you just need to implement the IOffsetDao interface from
the kom-core module.

Hbase1.0-Dao Implementation
---------------------------

1. Create an hbase table

.. code-block:: bash

    $ create 'offsettable', { NAME => 'd', VERSIONS => 1, COMPRESSION => 'SNAPPY'}


2. Create the DAO for use in the kafka offsetmanager

.. code-block:: java

    final Configuration hbaseConfiguration = HBaseConfiguration.create();
    hbaseConfiguration.addResource(new Path(str));

    final IOffsetStore dao = new Hbase1OffsetStore.Builder()
            .setHbaseConfiguration(hbaseConfiguration)
            .setOffsetTable("offsettable").build();


Kafka OffsetManager Examples
----------------------------

A java and scala example are included in the kom-examples module.

Both use the same strategy:

1. Setup a DAO
2. Setup the Kafka OffsetManager using the DAO.
3. Use the transform step to load-up the offsets being processed in this stream.
4. Use a final foreachrdd step to write the offsets with the offsetmanager.


