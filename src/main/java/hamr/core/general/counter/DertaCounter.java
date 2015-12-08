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
import java.util.Map;

import org.apache.hadoop.mapreduce.Reducer.Context;

import hamr.core.general.annotations.ReduceField;
import hamr.core.general.annotations.TargetField;
import hamr.core.general.bean.AnnotedBean;

public class DertaCounter extends Counter {

	private Map<String, Long> firstData;
	private Map<String, Long> lastData;
	static Map<Field, ReduceField> countingFields;
	static Map<Field, TargetField> targetFields;
	public DertaCounter(@SuppressWarnings("rawtypes") Context context) {
		super(context);
		firstData = new HashMap<String , Long>();
		lastData = new HashMap<String , Long>();
	}

	public void count(AnnotedBean ab) {
		if (countingFields == null) {
			countingFields = getCountFields(ab);
			for (Field f : countingFields.keySet()) {
				try {
					firstData.put(f.getName(), f.getLong(ab));
				} catch (IllegalArgumentException | IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		for (Field f : countingFields.keySet()) {
			try {
				lastData.put(f.getName(), f.getLong(ab));
			} catch (IllegalArgumentException | IllegalAccessException e) {
				// TODO Auto-generated catch block
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
      for(Field f : targetFields.keySet())
      {
    	  String fromField = targetFields.get(f).fromField();
    	  Long derta = lastData.get(fromField) - firstData.get(firstData);
    	  try {
			f.set(ret, derta);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
      }
      return true;
	}
	

}
