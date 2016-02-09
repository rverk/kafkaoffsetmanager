package org.ctrlr.kom.core;

import scala.Tuple2;
import scala.collection.JavaConverters$;

import java.util.Map;

class ScalaUtils {

    public static <K, V> scala.collection.immutable.Map<K, V> convertMapToImmutable(Map<K, V> m) {
        return JavaConverters$.MODULE$.mapAsScalaMapConverter(m).asScala().toMap(
                scala.Predef$.MODULE$.<Tuple2<K, V>>conforms()
        );
    }

}