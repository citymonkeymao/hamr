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
package hamr.core.general.counter;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.Reducer;

import hamr.core.general.annotations.ReduceField;
import hamr.core.general.annotations.TargetField;
import hamr.core.general.bean.AnnotedBean;
import hamr.core.utils.ReflectUtils;

public abstract class Counter {
	@SuppressWarnings("rawtypes")
	protected Reducer.Context context;
	protected List<Field> countFields;

	@SuppressWarnings("rawtypes")
	public Counter(Reducer.Context context) {
		this.context = context;
	}

	public void count(AnnotedBean key) {
	}

	public boolean end(AnnotedBean ret) {
		return false;
	}


	protected Map<Field, ReduceField> getCountFields(AnnotedBean ab) {
		Map<Field, ReduceField> ret = new HashMap<Field, ReduceField>();
		Set<Field> reduceFields = ab.getReduceField().keySet();
		for (Field f : reduceFields) {
			Class<? extends Counter>[] counterClasses = ab.getReduceField().get(f)
					.counterClass();
			for (Class<? extends Counter> cc : counterClasses) {
				if (cc == this.getClass()) {
					ret.put(f, ab.getReduceField().get(f));
				}
			}
		}
		return ret;
	}

	protected Map<Field, TargetField> getTargetFields(AnnotedBean ab) {
		Map<Field, TargetField> ret = new HashMap<Field, TargetField>();
		Field[] fields = ReflectUtils.getAllDeclareFields(ab.getClass(),
				Object.class);
		for (Field f : fields) {
			TargetField tf = f.getAnnotation(TargetField.class);
			if (tf != null && tf.generateFrom() == this.getClass()) {
				ret.put(f, tf);
			}
		}
		return ret;
	}

}
