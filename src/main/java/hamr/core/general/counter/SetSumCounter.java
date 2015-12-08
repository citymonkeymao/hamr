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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.Reducer.Context;

import hamr.core.general.annotations.ReduceField;
import hamr.core.general.annotations.TargetField;
import hamr.core.general.bean.AnnotedBean;

public class SetSumCounter extends Counter {
	private Map<String, Set<Object>> countings;
	static Map<Field, ReduceField> countFields;
	static Map<Field, TargetField> targetFields;

	@SuppressWarnings("rawtypes")
	public SetSumCounter(Context context) {
		super(context);
		countings = new HashMap<String, Set<Object>>();
	}

	@Override
	public void count(AnnotedBean ab) {

		if (countFields == null) {
			countFields = getCountFields(ab);
		}
		Set<Field> keys = countFields.keySet();
		for (Field f : keys) {
			if (!countings.containsKey(f.getName())) {
				countings.put(f.getName(), new HashSet<Object>());
			}
			try {
				countings.get(f.getName()).add(f.get(ab));
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean end(AnnotedBean ret) {
		if(ret == null)
		{
			return false;
		}
          if(targetFields == null)
          {
        	  targetFields = getTargetFields(ret);
          }
          Set<Field> fields =  targetFields.keySet();
          for(Field f : fields)
          {
        	  String fromField = f.getAnnotation(TargetField.class).fromField();
        	  Integer num = countings.get(fromField).size();
        	  try {
				f.set(ret, num);
			} catch (IllegalArgumentException | IllegalAccessException e) {
				e.printStackTrace();
			}
          }
          return true;
	}

}
