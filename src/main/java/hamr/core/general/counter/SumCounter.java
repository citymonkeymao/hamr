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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.Reducer.Context;

import hamr.core.general.annotations.TargetField;
import hamr.core.general.bean.AnnotedBean;

public class SumCounter extends Counter{

	private Integer count;
	
	@SuppressWarnings("rawtypes")
	public SumCounter(Context context) {
		super(context);
		count = 0;
	}
	public void count(AnnotedBean key)
	{
		count += 1;
	}
	public boolean end(AnnotedBean ret) {
		if(ret == null)
		{
			return false;
		}
		Map<Field , TargetField> targets = getTargetFields(ret);
		Set<Field> keys = targets.keySet();
		for(Field f : keys)
		{
			try {
				f.set(ret , count);
			} catch (IllegalArgumentException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

}
