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
package hamr.core.general.partitioner;
import java.lang.reflect.Field;

import org.apache.hadoop.mapreduce.Partitioner;

import hamr.core.general.bean.AnnotedBean;
public class GeneralPartitioner<K, V> extends Partitioner<K, V> {
	@Override
	public int getPartition(K key, V value, int numPartitions) {
		AnnotedBean k = (AnnotedBean)key;
		int hash = 0;
		//if no group fields, every item is sent to 1 partition
		if(k.getGroupFields().size() == 0)
		{
			return 1;
		}
		//use every group field to partition this item
		for (Field f : k.getGroupFields().keySet())
		{
			try {
				hash += f.get(key).hashCode();
			} catch (IllegalArgumentException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return hash & Integer.MAX_VALUE;
	}

}
