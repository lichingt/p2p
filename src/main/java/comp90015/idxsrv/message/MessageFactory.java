package comp90015.idxsrv.message;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A factory class to serialize and deserialize JSONSerializable
 * classes to and from strings.
 * @author aaron
 *
 */
public class MessageFactory {
	private static void checkIfSerializable(Object object) throws JsonSerializationException {
		if (Objects.isNull(object)) {
			throw new JsonSerializationException("The object to serialize is null");
		}
		Class<?> _class = object.getClass();
		if (!_class.isAnnotationPresent(JsonSerializable.class)) {
			throw new JsonSerializationException(
					"The class " + object.getClass().getSimpleName() + " is not annotated with JsonSerializable");
		}
		for (Field field : _class.getDeclaredFields()) {
			if (field.getName() == "_class") {
				throw new JsonSerializationException("The _class field cannot be used in a JsonSerializable class.");
			}
		}
		try {
			_class.getConstructor();
		} catch (NoSuchMethodException | SecurityException e) {
			throw new JsonSerializationException(
					"The object to serialize must contain a public initializer without arguments.");
		}
	}

	private static void initializeObject(Object object) throws Exception {
		Class<?> _class = object.getClass();
		for (Method method : _class.getDeclaredMethods()) {
			if (method.isAnnotationPresent(JsonSerializationInit.class)) {
				method.setAccessible(true);
				method.invoke(object);
			}
		}
	}

	private static JSONObject toJsonObject(Object object)
			throws JSONException, IllegalArgumentException, IllegalAccessException {
		Class<?> _class = object.getClass();
		JSONObject jobj = new JSONObject();
		jobj.put("_class", _class.getName());
		for (Field field : _class.getDeclaredFields()) {
			field.setAccessible(true);
			if (field.isAnnotationPresent(JsonElement.class)) {
				Class<?> fieldType = field.getType();
				if (fieldType.isAnnotationPresent(JsonSerializable.class)) {
					jobj.put(field.getName(), toJsonObject(field.get(object)));
				} else if(fieldType.isArray()){
					int l = Array.getLength(field.get(object));
					Object[] oa = new Object[l];
					for(int i=0;i<l;i++) {
						Object ao = Array.get(field.get(object), i);
						if(ao.getClass().isAnnotationPresent(JsonSerializable.class)) {
							oa[i]=toJsonObject(ao);
						} else {
							oa[i]=ao;
						}
					}
					jobj.put(field.getName(), oa);
				} else {
					jobj.put(field.getName(), field.get(object));
				}
			}
		}
		return jobj;
	}

	private static String toJsonString(Object object) throws Exception {
		JSONObject jobj = toJsonObject(object);
		return jobj.toString();
	}

	private static Object fromJsonObj(JSONObject jobj) throws JsonSerializationException {
		try {
			Class<?> _class = Class.forName(jobj.getString("_class"));
			if(!_class.isAnnotationPresent(JsonSerializable.class)) {
				throw new JsonSerializationException("The class represented by the JSON object is not json serializable.");
			}
			Constructor<?> constructor = _class.getConstructor();
			Object obj = constructor.newInstance();
			for (Field field:_class.getDeclaredFields()) {
				field.setAccessible(true);
				if (field.isAnnotationPresent(JsonElement.class)) {
					Class<?> fieldType = field.getType();
					String fieldName = field.getName(); 
					if(jobj.has(fieldName)){
						Object jsonFieldVal = jobj.get(fieldName);
						Class<?> jsonFieldType = jsonFieldVal.getClass();
						if(fieldType==Long.class && jsonFieldType==Integer.class) {
							field.set(obj, jobj.getLong(fieldName));
						} else if(fieldType.isAssignableFrom(jsonFieldType)) {
							field.set(obj, jsonFieldVal);
						} else if(jsonFieldType==JSONObject.class){
							field.set(obj, fromJsonObj((JSONObject)jsonFieldVal));
						} else if(jsonFieldType==JSONArray.class && fieldType.isArray()){
							JSONArray ja = (JSONArray) jsonFieldVal;
							Object arrObj = Array.newInstance(fieldType.getComponentType(), ja.length());
							for(int i=0;i<ja.length();i++) {
								Object obji = ja.get(i);
								Class<?> objiType = obji.getClass();
								if(objiType==JSONObject.class) {
									obji=fromJsonObj((JSONObject)obji);
									objiType=obji.getClass();
								}
								if(objiType==fieldType.getComponentType() ||
										(objiType==Integer.class && fieldType.getComponentType()==Long.class)) {
									Array.set(arrObj, i, obji);
								} else {
									throw new JsonSerializationException("Array component types do not match");
								}
							}
							Object[] arr = (Object[]) arrObj;
							if(fieldType.isAssignableFrom(arr.getClass())) {
								field.set(obj, arr);
							} else {
								throw new JsonSerializationException("Array of type "+fieldType+" is not supported");
							}
						} else {
							throw new JsonSerializationException("Field was of incorrect type: "+fieldName);
						}
					} else {
						throw new JsonSerializationException("Required field is not present: "+fieldName);
					}
		        }
			}
			return obj;
		} catch (JSONException e) {
			throw new JsonSerializationException(e.getMessage());
		} catch (ClassNotFoundException e) {
			throw new JsonSerializationException(e.getMessage());
		} catch (NoSuchMethodException e) {
			throw new JsonSerializationException(e.getMessage());
		} catch (SecurityException e) {
			throw new JsonSerializationException(e.getMessage());
		} catch (InstantiationException e) {
			throw new JsonSerializationException(e.getMessage());
		} catch (IllegalAccessException e) {
			throw new JsonSerializationException(e.getMessage());
		} catch (IllegalArgumentException e) {
			throw new JsonSerializationException(e.getMessage());
		} catch (InvocationTargetException e) {
			throw new JsonSerializationException(e.getMessage());
		}
		
	}

	/**
	 * Serialize a JSONSerializable object into a string.
	 * @param object
	 * @return
	 * @throws JsonSerializationException
	 */
	public static String serialize(Object object) throws JsonSerializationException {
		try {
			checkIfSerializable(object);
			initializeObject(object);
			return toJsonString(object);
		} catch (Exception e) {
			throw new JsonSerializationException(e.getMessage());
		} 
	}

	/**
	 * Deserialize a string into a JSONSerializable object.
	 * @param str
	 * @return
	 * @throws JsonSerializationException
	 */
	public static Object deserialize(String str) throws JsonSerializationException {
		try {
			JSONObject jobj = new JSONObject(str);
			return fromJsonObj(jobj);
		} catch (JSONException e) {
			throw new JsonSerializationException(e.getMessage());
		}
	}
}
