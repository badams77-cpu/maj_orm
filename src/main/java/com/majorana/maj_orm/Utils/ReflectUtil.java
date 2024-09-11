package com.majorana.maj_orm.Utils;

import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Locale;

public class ReflectUtil {

	/**
	 * Describe an object to a writer
	 *
	 * @param object
	 * @param out
	 * @param lineBreak
	 */

	public static void describeInstanceAndValues(Object object, Writer out, String lineBreak) {
		PrintWriter pout = new PrintWriter(out);
		Class<?> clazz = object.getClass();

		Constructor<?>[] constructors = clazz.getDeclaredConstructors();
		Field[] fields = clazz.getDeclaredFields();
		Method[] methods = clazz.getDeclaredMethods();
		Class[] inter = clazz.getInterfaces();

		pout.println("Description for class: " + clazz.getName()+ " and instance "+object.hashCode()+lineBreak);
		pout.println(lineBreak);
		pout.println("Summary"+lineBreak);
		pout.println("-----------------------------------------"+lineBreak);
		System.out.println("Constructors: " + (constructors.length+lineBreak));
		pout.println("Fields: " + (fields.length)+lineBreak);
		pout.println("Methods: " + (methods.length)+lineBreak);

		pout.println(lineBreak);
		pout.println(lineBreak);
		pout.println("Details"+lineBreak);
		pout.println("-----------------------------------------"+lineBreak);

		if (inter.length >0){
			pout.println(lineBreak);
			pout.println("Implements: "+lineBreak);
			for(int i=0; i<inter.length; i++){
				pout.println(inter[i].getName()+lineBreak);
				pout.println(" AKA: "+inter[i].getSimpleName()+lineBreak);
			}
		}

		if (constructors.length > 0) {
			pout.println(lineBreak);
			pout.println("Constructors:"+lineBreak);
			for (Constructor<?> constructor : constructors) {
				pout.println(constructor+lineBreak);
			}
		}

		if (fields.length > 0) {
			pout.println(lineBreak);
			pout.println("Fields:"+lineBreak);
			for (Field field : fields) {
				pout.println(field);
				try {
					Method getter = findGetterMethodForField(field, methods);
					if (getter!=null) {
						Object ob = invokeGetter(object, getter);
						pout.println(ob);
					}
				} catch (Exception e){}
				pout.println(lineBreak);
			}
		}

		if (methods.length > 0) {
			pout.println(lineBreak);
			pout.println("Methods:"+lineBreak);
			for (Method method : methods) {
				pout.println(method);
				pout.println(lineBreak);
			}
		}
		pout.close();
	}

	public static Object invokeGetter(Object obj,Method getter) throws IllegalAccessException, IllegalArgumentException , InvocationTargetException
	{
		Object f = getter.invoke(obj);
		return f;
	}

	public static Method findGetterMethodForField( Field field, Method[] methods){
		String fname = field.getName();
		String capName = fname.substring(0,0).toUpperCase(Locale.ROOT)+fname.substring(1);
		for(Method method : methods){
			String mname = method.getName();
			if (mname.equals("is"+capName) || mname.equals("get"+capName)){
				return method;
			}
		}
		return null;
	}


	  public static void describeInstance(Object object, Writer out) {
		  PrintWriter pout = new PrintWriter(out);
	    Class<?> clazz = object.getClass();
	    
	    Constructor<?>[] constructors = clazz.getDeclaredConstructors();
	    Field[] fields = clazz.getDeclaredFields();
	    Method[] methods = clazz.getDeclaredMethods();
        Class[] inter = clazz.getInterfaces();
	    
	    pout.println("Description for class: " + clazz.getName());
	    pout.println();
	    pout.println("Summary");
	    pout.println("-----------------------------------------");
	    System.out.println("Constructors: " + (constructors.length));
	    pout.println("Fields: " + (fields.length));
	    pout.println("Methods: " + (methods.length));

	    pout.println();
	    pout.println();
	    pout.println("Details");
	    pout.println("-----------------------------------------");

	    if (inter.length >0){
	    	pout.println();
	    	pout.println("Implements: ");
	    	for(int i=0; i<inter.length; i++){
	    		pout.println(inter[i].getName());
	    		pout.println(" AKA: "+inter[i].getSimpleName());
	    	}
	    }
	    
	    if (constructors.length > 0) {
	      pout.println();
	      pout.println("Constructors:");
	      for (Constructor<?> constructor : constructors) {
	        pout.println(constructor);
	      }
	    }

	    if (fields.length > 0) {
	      pout.println();
	      pout.println("Fields:");
	      for (Field field : fields) {
	        pout.println(field);
	      }
	    }

	    if (methods.length > 0) {
	      pout.println();
	      pout.println("Methods:");
	      for (Method method : methods) {
	        pout.println(method);
	      }
	    }
	    pout.close();
	  }
	  
}


	
