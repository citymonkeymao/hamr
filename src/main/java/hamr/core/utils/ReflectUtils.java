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
package hamr.core.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReflectUtils {

	public static Field[] getAllDeclareFields(Class<?> clazz) {
		return getAllDeclareFields(clazz, null);
	}

	public static Field[] getAllDeclareFields(Class<?> clazz, Class<?> topClass) {
		List<Field> fields = new ArrayList<Field>();
		if (!allFieldsCache.containsKey(clazz.getName())) {
			Class<?> cur_clazz = clazz;
			Class<?> last_clazz = null;
			while ((topClass == null && cur_clazz != null)
					|| (topClass != null && cur_clazz != null
							&& last_clazz != null && topClass != last_clazz)
					|| last_clazz == null) {
				last_clazz = cur_clazz;
				Field[] thisClassFields = cur_clazz.getDeclaredFields();
				for (Field field : thisClassFields) {
					fields.add(field);
				}
				cur_clazz = cur_clazz.getSuperclass();
			}
			synchronized (allFieldsCache) {
				allFieldsCache.put(clazz.getName(), fields);
			}
		} else {
			fields = allFieldsCache.get(clazz.getName());
		}
		return fields.toArray(new Field[0]);
	}

	public static Field getDeclareFieldByName(Class<?> clazz,
			Class<?> topClass, String fieldName) throws NoSuchFieldException {
		Field field = null;
		String fieldKey = clazz.getName() + "." + fieldName;
		if (!fieldsCache.containsKey(fieldKey)) {
			Class<?> cur_clazz = clazz;
			Class<?> last_clazz = null;
			while ((topClass == null && cur_clazz != null)
					|| (topClass != null && cur_clazz != null
							&& last_clazz != null && topClass != last_clazz)
					|| last_clazz == null) {
				last_clazz = cur_clazz;
				try {
					field = cur_clazz.getDeclaredField(fieldName);
				} catch (NoSuchFieldException e) {
				}
				if (field != null) {
					break;
				} else {
					cur_clazz = cur_clazz.getSuperclass();
				}
			}
			synchronized (fieldsCache) {
				fieldsCache.put(fieldKey, field);
			}
		} else {
			field = fieldsCache.get(fieldKey);
		}
		if (field == null) {
			throw new NoSuchFieldException(fieldName + "不存在");
		}
		return field;
	}

	private static Map<String, List<Field>> allFieldsCache = new HashMap<String, List<Field>>();

	private static Map<String, Field> fieldsCache = new HashMap<String, Field>();
}
