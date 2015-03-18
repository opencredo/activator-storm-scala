package com.opencredo.storm.utils

import scala.collection.JavaConverters._

object Fields {
    def apply(values: String*) = new backtype.storm.tuple.Fields(values.asJava)
}