package cc.yuerblog;

import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class Hadoop {
    public static void main(String[] args)  {
        String mapperClass = args[0];
        String reducerClass = args[1];

        try {
            URL []classpath = new URL[] {
                    new URL("file:/Users/smzdm/IdeaProjects/classloader-demo/mapreduce/target/mapreduce-1.0-SNAPSHOT.jar")
            };

            // 在jar中加载class
            URLClassLoader loader = new URLClassLoader(classpath);
            Class mapperClz = loader.loadClass(mapperClass);
            Class reducerClz = loader.loadClass(reducerClass);

            // 反射构造对象
            Object mapper = mapperClz.newInstance();
            Object reducer = reducerClz.newInstance();

            // 反射方法
            Method map = mapperClz.getMethod("map");
            Method reduce = reducerClz.getMethod("reduce");

            // 执行
            map.invoke(mapper);
            reduce.invoke(reducer);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
