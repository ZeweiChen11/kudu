// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.spark.tools

import org.apache.kudu.client.KuduClient
import org.apache.kudu.spark.tools.ImportExportKudu.ArgsCls
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.kudu.spark.kudu._
import com.databricks.spark.avro
import com.google.common.annotations.VisibleForTesting
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType

object ImportExportKudu {
  val LOG: Logger = LoggerFactory.getLogger(ImportExportKudu.getClass)

  def fail(msg: String): Nothing = {
    System.err.println(msg)
    sys.exit(1)
  }

  def usage: String =
    s"""
       | Usage: --operation=import/export --format=<data-format(csv,parquet,avro)> --master-addrs=<master-addrs> --path=<path> --table-name=<table-name>
       |    where
       |      operation: import or export data from or to Kudu tables, default: import
       |      format: specify the format of data want to import/export, the following formats are supported csv,parquet,avro default:csv
       |      masterAddrs: comma separated addresses of Kudu master nodes, default: localhost
       |      path: path to input or output for import/export operation, default: file://
       |      tableName: table name to import/export, default: ""
       |      columns: columns name for select statement on export from kudu table, default: *
       |      delimiter: delimiter for csv import/export, default: ,
       |      header: header for csv import/export, default:false
     """.stripMargin

  case class ArgsCls(operation: String = "import",
                     format: String = "csv",
                     masterAddrs: String = "localhost",
                     path: String = "file://",
                     table: String = "",
                     tableName: String = "",
                     columns: String = "*",
                     delimiter: String = ",",
                     header: String = "false",
                     inferschema: String="false"
                    )

  object ArgsCls {
    private def parseInner(options: ArgsCls, args: List[String]): ArgsCls = {
      LOG.info(args.mkString(","))
      args match {
        case Nil => options
        case "--help" :: _ =>
          System.err.println(usage)
          sys.exit(0)
        case flag :: Nil => fail(s"flag $flag has no value\n$usage")
        case flag :: value :: tail =>
          val newOptions: ArgsCls = flag match {
            case "--operation" => options.copy(operation = value)
            case "--format" => options.copy(format = value)
            case "--master-addrs" => options.copy(masterAddrs = value)
            case "--path" => options.copy(path = value)
            case "--table" => options.copy(table = value)
            case "--table-name" => options.copy(tableName = value)
            case "--columns" => options.copy(columns = value)
            case "--delimiter" => options.copy(delimiter = value)
            case "--header" => options.copy(header = value)
            case "--inferschema" => options.copy(inferschema = value)
            case _ => fail(s"unknown argument given $flag")
          }
          parseInner(newOptions, tail)
      }
    }

    def parse(args: Array[String]): ArgsCls = {
      parseInner(ArgsCls(), args.flatMap(_.split('=')).toList)
    }
  }
}

object ImportExportFiles {

  import ImportExportKudu.{LOG, fail}

  var sqlContext: SQLContext = _
  var kuduOptions: Map[String, String] = _

  def run(args: ArgsCls, sc: SparkContext, sqlContext: SQLContext): Unit = {
    val kc = new KuduContext(args.masterAddrs, sc)
    val applicationId = sc.applicationId

    val client: KuduClient = kc.syncClient
    if (!client.tableExists(args.tableName)) {
      fail(args.tableName + s" table doesn't exist")
    }

    kuduOptions = Map(
      "kudu.table" -> args.tableName,
      "kudu.master" -> args.masterAddrs)

    args.operation match {
      case "import" =>
        args.format match {
          case "csv" =>
            val df = sqlContext.read.option("header", args.header).option("delimiter", args.delimiter).csv(args.path)
            df.show()
            // convert column type for tpch tables
            if (args.table == "customer"){
              val df1 = df.withColumn("c0_tmp", df.col("_c0").cast(LongType))
                .drop("_c0")
                .withColumnRenamed("c0_tmp", "_c0")
              val df2 = df1.withColumn("c3_tmp", df.col("_c3").cast(LongType))
                .drop("_c3")
                .withColumnRenamed("c3_tmp", "_c3")
              val df3 = df2.withColumn("c5_tmp", df.col("_c5").cast(DoubleType))
                .drop("_c5")
                .withColumnRenamed("c5_tmp", "_c5")
              kc.upsertRows(df3, args.tableName)
            } else if (args.table == "lineitem") {
              val df1 = df.withColumn("c0_tmp", df.col("_c0").cast(LongType))
                .drop("_c0")
                .withColumnRenamed("c0_tmp", "_c0")
              val df2 = df1.withColumn("c1_tmp", df.col("_c1").cast(LongType))
                .drop("_c1")
                .withColumnRenamed("c1_tmp", "_c1")
              val df3 = df2.withColumn("c2_tmp", df.col("_c2").cast(LongType))
                .drop("_c2")
                .withColumnRenamed("c2_tmp", "_c2")
              val df4 = df3.withColumn("c3_tmp", df.col("_c3").cast(IntegerType))
                .drop("_c3")
                .withColumnRenamed("c3_tmp", "_c3")
              val df5 = df4.withColumn("c4_tmp", df.col("_c4").cast(DoubleType))
                .drop("_c4")
                .withColumnRenamed("c4_tmp", "_c4")
              val df6 = df5.withColumn("c5_tmp", df.col("_c5").cast(DoubleType))
                .drop("_c5")
                .withColumnRenamed("c5_tmp", "_c5")
              val df7 = df6.withColumn("c6_tmp", df.col("_c6").cast(DoubleType))
                .drop("_c6")
                .withColumnRenamed("c6_tmp", "_c6")
              val df8 = df7.withColumn("c7_tmp", df.col("_c7").cast(DoubleType))
                .drop("_c7")
                .withColumnRenamed("c7_tmp", "_c7")
              kc.upsertRows(df8, args.tableName)
            } else if (args.table == "nation"){
              val df1 = df.withColumn("c0_tmp", df.col("_c0").cast(LongType))
                .drop("_c0")
                .withColumnRenamed("c0_tmp", "_c0")
              val df2 = df1.withColumn("c2_tmp", df.col("_c2").cast(LongType))
                .drop("_c2")
                .withColumnRenamed("c2_tmp", "_c2")
              kc.upsertRows(df2, args.tableName)
            } else if (args.table == "orders"){
              val df1 = df.withColumn("c0_tmp", df.col("_c0").cast(LongType))
                .drop("_c0")
                .withColumnRenamed("c0_tmp", "_c0")
              val df2 = df1.withColumn("c1_tmp", df.col("_c1").cast(LongType))
                .drop("_c1")
                .withColumnRenamed("c1_tmp", "_c1")
              val df3 = df2.withColumn("c3_tmp", df.col("_c3").cast(DoubleType))
                .drop("_c3")
                .withColumnRenamed("c3_tmp", "_c3")
              val df4 = df3.withColumn("c7_tmp", df.col("_c7").cast(IntegerType))
                .drop("_c7")
                .withColumnRenamed("c7_tmp", "_c7")
              kc.upsertRows(df4, args.tableName)
            } else if (args.table == "part"){
              val df1 = df.withColumn("c0_tmp", df.col("_c0").cast(LongType))
                .drop("_c0")
                .withColumnRenamed("c0_tmp", "_c0")
              val df2 = df1.withColumn("c5_tmp", df.col("_c5").cast(IntegerType))
                .drop("_c5")
                .withColumnRenamed("c5_tmp", "_c5")
              val df3 = df2.withColumn("c7_tmp", df.col("_c7").cast(DoubleType))
                .drop("_c7")
                .withColumnRenamed("c7_tmp", "_c7")
              kc.upsertRows(df3, args.tableName)
            } else if (args.table == "partsupp"){
              val df1 = df.withColumn("c0_tmp", df.col("_c0").cast(LongType))
                .drop("_c0")
                .withColumnRenamed("c0_tmp", "_c0")
              val df2 = df1.withColumn("c1_tmp", df.col("_c1").cast(LongType))
                .drop("_c1")
                .withColumnRenamed("c1_tmp", "_c1")
              val df3 = df2.withColumn("c2_tmp", df.col("_c2").cast(IntegerType))
                .drop("_c2")
                .withColumnRenamed("c2_tmp", "_c2")
              val df4 = df3.withColumn("c3_tmp", df.col("_c3").cast(DoubleType))
                .drop("_c3")
                .withColumnRenamed("c3_tmp", "_c3")
              kc.upsertRows(df4, args.tableName)
            } else if (args.table == "region"){
              val df1 = df.withColumn("c0_tmp", df.col("_c0").cast(LongType))
                .drop("_c0")
                .withColumnRenamed("c0_tmp", "_c0")
              kc.upsertRows(df1, args.tableName)

            } else if (args.table == "supplier"){
              val df1 = df.withColumn("c0_tmp", df.col("_c0").cast(LongType))
                .drop("_c0")
                .withColumnRenamed("c0_tmp", "_c0")
              val df2 = df1.withColumn("c3_tmp", df.col("_c3").cast(LongType))
                .drop("_c3")
                .withColumnRenamed("c3_tmp", "_c3")
              val df3 = df2.withColumn("c5_tmp", df.col("_c5").cast(DoubleType))
                .drop("_c5")
                .withColumnRenamed("c5_tmp", "_c5")
              kc.upsertRows(df3, args.tableName)
            }
            else kc.upsertRows(df, args.tableName)

          case "parquet" =>
            val df = sqlContext.read.parquet(args.path)
            kc.upsertRows(df, args.tableName)
          case "avro" =>
            val df = sqlContext.read.format("com.databricks.spark.avro").load(args.path)
            kc.upsertRows(df, args.tableName)
          case _ => fail(args.format + s"unknown argument given ")
        }
      case "export" =>
        val df = sqlContext.read.options(kuduOptions).kudu.select(args.columns);
        args.format match {
          case "csv" =>
            df.write.format("com.databricks.spark.csv").option("header", args.header).option("delimiter",
              args.delimiter).save(args.path)
          case "parquet" =>
            df.write.parquet(args.path)
          case "avro" =>
            df.write.format("com.databricks.spark.avro").save(args.path)
          case _ => fail(args.format + s"unknown argument given  ")
        }
      case _ => fail(args.operation + s"unknown argument given ")
    }
  }
  /**
    * Entry point for testing. SparkContext is a singleton,
    * so tests must create and manage their own.
    */
  @VisibleForTesting
  def testMain(args: Array[String], sc: SparkContext): Unit = {
    sqlContext = new SQLContext(sc)
    run(ArgsCls.parse(args), sc, sqlContext)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Import or Export CSV files from/to Kudu ")
    val sc = new SparkContext(conf)
    testMain(args, sc)
  }
}

