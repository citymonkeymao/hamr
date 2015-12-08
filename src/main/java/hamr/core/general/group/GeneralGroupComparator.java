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
package hamr.core.general.group;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;

import hamr.core.general.annotations.GroupField;
import hamr.core.general.bean.AnnotedBean;
import hamr.core.general.fieldComparators.FieldComparator;

public class GeneralGroupComparator extends WritableComparator {
	@SuppressWarnings("rawtypes")
	private WritableComparable key1;
	@SuppressWarnings("rawtypes")
	private WritableComparable key2;
	private final DataInputBuffer buffer;

	protected GeneralGroupComparator() {
		buffer = new DataInputBuffer();
		// super(conf.getMapOutputKeyClass().asSubclass(WritableComparable.class)
		// , true);
	}

	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable a, WritableComparable b) {
		Integer ret = 0;
		AnnotedBean beanA = (AnnotedBean) a;
		AnnotedBean beanB = (AnnotedBean) b;
		Map<Field, FieldComparator> groupFields = beanA.getGroupFields();
		// if there is no group field, every key is equal
		if (groupFields.size() == 0) {
			return ret;
		}
		for (Field f : groupFields.keySet()) {
			try {
				// any group field is not equal, the ret would not be 0
				ret = groupFields.get(f).compare(f.get(beanA), f.get(beanB))
						* (1 << f.getAnnotation(GroupField.class).level());
			} catch (IllegalArgumentException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return ret;
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		if (key1 == null) {
			Configuration conf = getConf();
			JobConf jcon = new JobConf(conf);
			try {
				key1 = jcon.getMapOutputKeyClass()
						.asSubclass(WritableComparable.class).newInstance();
				key2 = jcon.getMapOutputKeyClass()
						.asSubclass(WritableComparable.class).newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		try {
			buffer.reset(b1, s1, l1); // parse key1
			key1.readFields(buffer);

			buffer.reset(b2, s2, l2); // parse key2
			key2.readFields(buffer);

			buffer.reset(null, 0, 0); // clean up reference
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return compare(key1, key2); // compare them
	}

}
