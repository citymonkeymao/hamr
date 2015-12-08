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
package hamr.core.general.reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import hamr.core.general.annotations.Counters;
import hamr.core.general.annotations.Preduce;
import hamr.core.general.bean.AnnotedBean;
import hamr.core.general.counter.Counter;
import hamr.core.general.preducer.Preducer;

public class GeneralReducer extends
		Reducer<AnnotedBean, NullWritable, AnnotedBean, NullWritable> {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void reduce(
			AnnotedBean key,
			Iterable<NullWritable> value,
			Reducer<AnnotedBean, NullWritable, AnnotedBean, NullWritable>.Context context) {

		Preducer predu = null;
		List<Counter> counters = null;
		AnnotedBean preduced = null;
		AnnotedBean result = null;
		Iterator<NullWritable> iter = value.iterator();
		while (iter.hasNext()) {
			iter.next();
			// build preducer
			Class preducerClass = null;
			if (predu == null) {
				Preduce preduce = key.getClass()
						.getAnnotation(Preduce.class);
				if (preduce == null || preduce.preducer() == null) {
					// default preducer
					preducerClass = Preducer.class;
				}
				else
				{
					preducerClass = preduce.preducer();
				}
				try {
					predu = (Preducer) preducerClass.getDeclaredConstructor(
							Reducer.Context.class).newInstance(context);
				} catch (InstantiationException | IllegalAccessException
						| IllegalArgumentException | InvocationTargetException
						| NoSuchMethodException | SecurityException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			// preduce data
			preduced = predu.preduce(key);
			if(result == null)
			{
				result = preduced;
			}
			if(preduced == null)
			{
				continue;
			}

			// build counters
			if (counters == null) {
				counters = new ArrayList<Counter>();
				Class[] counterClasses = preduced.getClass()
						.getAnnotation(Counters.class).counters();
				for (Class cc : counterClasses) {
					try {
						counters.add((Counter) cc.getConstructor(Context.class)
								.newInstance(context));
					} catch (InstantiationException | IllegalAccessException
							| IllegalArgumentException
							| InvocationTargetException | NoSuchMethodException
							| SecurityException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

			// send each data to each counter;

			for (Counter c : counters) {
				c.count(preduced);
			}
		}
		//if all preduced is invalid
		if(counters == null)
		{
			return ;
		}
		//end each counter and decide wether to invoke write or not
		boolean needWrite = false;
		for (Counter c : counters) {
			if(c.end(result))
			{
				needWrite = true;
			}
		}
		
		//need write then write
		if(needWrite)
		{
			try {
				context.write(result, NullWritable.get());
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}
