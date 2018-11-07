/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package myflink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class Filter {

	public static void main(String[] args) throws Exception {
		Path filePath = Path.fromLocalFile(new File("/Users/wuchong/Downloads/UserBehavior.csv"));
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		TypeInformation[] fieldTypes = new TypeInformation[]{Types.LONG, Types.LONG, Types.LONG, Types.STRING, Types.LONG};
		TupleTypeInfo<Tuple5<Long, Long, Long, String, Long>> tupleType = new TupleTypeInfo<>(Types.LONG, Types.LONG, Types.LONG, Types.STRING, Types.LONG);
		TupleCsvInputFormat<Tuple5<Long, Long, Long, String, Long>> csvInput = new TupleCsvInputFormat<>(filePath, tupleType);

		DataStream<Tuple5<Long, Long, Long, String, Long>> dataSource = env.createInput(csvInput, tupleType);




//		dataSource.assignTimestampsAndWatermarks()

		dataSource.filter(new FilterFunction<Tuple5<Long, Long, Long, String, Long>>() {
			@Override
			public boolean filter(Tuple5<Long, Long, Long, String, Long> t) throws Exception {
				return t.f4 >= 1511658000 && t.f4 <= 1511690400;
			}
		}).writeUsingOutputFormat(new CsvOutputFormat(Path.fromLocalFile(new File("/Users/wuchong/Downloads/UserBehavior9hour.csv")))).setParallelism(1);

		env.execute();

	}
}
