/*
  Copyright (c) 2015, Yiju Wei. 

  HAMR is a Frame let you use annotations to describe and execute a MapReduce process.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */
package hamr.core.general.mapper;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Mapper;

import hamr.core.general.annotations.Generator;
import hamr.core.general.bean.AnnotedBean;
import hamr.core.general.keyGenerator.KeyGenerator;

public class GeneralMapper extends
		Mapper<Object, Object, AnnotedBean, NullWritable> {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void map(Object key, Object value, Mapper.Context context)
			throws IOException, InterruptedException {
		Class<? extends KeyGenerator> kgClass = context
				.getConfiguration()
				.getClass(JobContext.MAP_OUTPUT_KEY_CLASS, null,
						AnnotedBean.class).getAnnotation(Generator.class)
				.keyGeneratorClass();
		KeyGenerator kg;
		try {
			kg = kgClass.getConstructor(Mapper.Context.class).newInstance(
					context);
			List<AnnotedBean> generated = kg.generate(key, value);
			if (generated != null) {
				for (int i = 0; i < generated.size(); i++) {
					context.write(generated.get(i), NullWritable.get());
				}
			}
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
	}

}
