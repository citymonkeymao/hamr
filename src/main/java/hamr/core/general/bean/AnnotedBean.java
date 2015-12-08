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
package hamr.core.general.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.WritableComparable;

import hamr.core.general.annotations.GroupField;
import hamr.core.general.annotations.ReduceField;
import hamr.core.general.annotations.SkipIO;
import hamr.core.general.annotations.SortField;
import hamr.core.general.fieldComparators.FieldComparator;
import hamr.core.utils.ReflectUtils;

public abstract class AnnotedBean implements WritableComparable<AnnotedBean> {
	static Map<Field, SortField> sortFields;
	static Map<Field, FieldComparator> comparators;
	static Map<Field, ReduceField> reduceFields;
	static Map<Field, FieldComparator> groupFields;

	public Map<Field, FieldComparator> getGroupFields() {
		if (groupFields == null) {
			Field[] fields = ReflectUtils.getAllDeclareFields(this.getClass(),
					Object.class);
			groupFields = new HashMap<Field, FieldComparator>();
			for (Field f : fields) {
				FieldComparator fc = null;
				try {
					if (f.getAnnotation(GroupField.class) == null) {
						continue;
					}
					fc = (FieldComparator) f.getAnnotation(GroupField.class)
							.comparator().newInstance();
				} catch (InstantiationException | IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				groupFields.put(f, fc);
			}
		}
		return groupFields;
	}

	public Map<Field, SortField> getSortingFields() {
		if (sortFields != null) {
			return sortFields;
		}
		Field[] fields = ReflectUtils.getAllDeclareFields(this.getClass(),
				Object.class);
		sortFields = new HashMap<Field, SortField>();
		for (Field f : fields) {
			SortField sortField = f.getAnnotation(SortField.class);
			if (sortField == null) {
				continue;
			} else {
				sortFields.put(f, sortField);
			}
		}
		return sortFields;
	}

	public Map<Field, ReduceField> getReduceField() {

		if (reduceFields != null) {
			return reduceFields;
		}
		Field[] fields = ReflectUtils.getAllDeclareFields(this.getClass(),
				Object.class);
		reduceFields = new HashMap<Field, ReduceField>();
		for (Field f : fields) {
			ReduceField reduceField = f.getAnnotation(ReduceField.class);
			if (reduceField == null) {
				continue;
			} else {
				reduceFields.put(f, reduceField);
			}
		}
		return reduceFields;
	}

	private Map<Field, FieldComparator> getComparators() {
		if (comparators != null) {
			return comparators;
		}
		comparators = new HashMap<Field, FieldComparator>();
		Map<Field, SortField> fields = getSortingFields();
		for (Field f : fields.keySet()) {
			Class<? extends FieldComparator> comparatorClass = sortFields
					.get(f).comparator();
			try {
				FieldComparator comparator = comparatorClass.newInstance();
				comparators.put(f, comparator);
			} catch (InstantiationException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return comparators;
	}

	protected Boolean isSkip(Field field) {
		SkipIO skip = field.getAnnotation(SkipIO.class);
		if (null != skip) {
			return true;
		}
		return false;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Field[] fields = ReflectUtils.getAllDeclareFields(this.getClass(),
				Object.class);
		Field field = null;
		for (Field field2 : fields) {
			field = field2;
			try {
				field.setAccessible(true);
				if (isSkip(field)) {
					continue;
				}
				if (field.getType() == String.class) {
					field.set(this, in.readUTF());
				}
				if (field.getType() == Integer.class) {
					field.set(this, in.readInt());
				}
				if (field.getType() == Float.class) {
					field.set(this, in.readFloat());
				}
				if (field.getType() == Double.class) {
					field.set(this, in.readDouble());
				}
				if (field.getType() == Long.class) {
					field.set(this, in.readLong());
				} else {
					continue;
				}
			} catch (IllegalArgumentException | IllegalAccessException e) {
				e.printStackTrace();
			} catch (EOFException e) {
//				System.out.println(this.toString());
				// for (int j = 0; j < fields.length; j++) {
				// Field field2 = fields[j];
				// System.out.println(field.getName());
				// }
			} catch (Exception e) {
//				System.out.println(field.getName());
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Field[] fields = ReflectUtils.getAllDeclareFields(this.getClass(),
				Object.class);
		for (Field field : fields) {
			try {
				field.setAccessible(true);
				Object property = field.get(this);
				if (isSkip(field)) {
					continue;
				}

				if (field.getType() == String.class) {
					if (property == null) {
						out.writeUTF("");
					} else {
						out.writeUTF(String.valueOf(property));
					}
				}
				if (field.getType() == Integer.class) {
					if (property == null) {
						out.writeInt(0);
					} else {
						out.writeInt((Integer) property);
					}
				}
				if (field.getType() == Long.class) {
					if (property == null) {
						out.writeLong(0);
					} else {
						out.writeLong((Long) property);
					}
				}
				if (field.getType() == Float.class) {
					if (property == null) {
						out.writeFloat(0f);
					} else {
						out.writeFloat((Float) property);
					}
				}
				if (field.getType() == Double.class) {
					if (property == null) {
						out.writeDouble(0);
					} else {
						out.writeDouble((Double) property);
					}
				}
			} catch (IllegalArgumentException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public int compareTo(AnnotedBean o) {
		int ret = 0;
		Set<Field> allSortingFields = getComparators().keySet();
		if (allSortingFields.size() == 0) {
			return 0;
		}
		for (Field f : allSortingFields) {
			FieldComparator comparator = getComparators().get(f);// null;
			// if (comparators.containsKey(f)) {
			// comparator = comparators.get(f);
			// } else {
			// comparator = new FieldComparator();
			// }
			int level = getSortingFields().get(f).level();
			try {
				ret += comparator.compare(f.get(this), f.get(o)) * (1 << level);
			} catch (IllegalArgumentException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return (int) Math.signum(ret);

	}

}
