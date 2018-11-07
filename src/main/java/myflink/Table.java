///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package myflink;
//
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.api.Types;
//import org.apache.flink.table.api.java.BatchTableEnvironment;
//import org.apache.flink.table.sinks.CsvTableSink;
//import org.apache.flink.table.sources.CsvTableSource;
//
//public class Table {
//
//	public static void main(String[] args) throws Exception {
//		ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
//		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
//
//		CsvTableSource csv = CsvTableSource
//				.builder()
//				.path("/Users/wuchong/Downloads/UserBehavior.csv")
//				.field("f0", Types.LONG())
//				.field("f1", Types.LONG())
//				.field("f2", Types.LONG())
//				.field("f3", Types.STRING())
//				.field("ts", Types.LONG())
//				.build();
//
//		tEnv.registerTableSource("csv", csv);
//
//		CsvTableSink sink = new CsvTableSink("/Users/wuchong/Downloads/UserBehavior9hour.csv", ",");
//
//		tEnv.sqlQuery("SELECT * FROM csv WHERE ts >= 1511658000 AND ts <= 1511690400 ORDER BY ts ASC")
//		    .writeToSink(sink);
//	}
//}
