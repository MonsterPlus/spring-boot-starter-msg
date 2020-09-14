package com.huake.msg.kafka.utils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.springframework.core.annotation.Order;
import org.springframework.core.io.support.SpringFactoriesLoader;

/**
 * 从spring.factories 文件中获取接口实现类工具
 *
 * @Author zkj_95@163.com
 * @date 2020年9月7日
 *
 */
public class SpringFactoriesUtils {
	public static <T> T loadFactories(Class<T> factoryClass) {
		List<T> clazzList = SpringFactoriesLoader.loadFactories(factoryClass, null);
		Iterator<T> iterator = clazzList.iterator();
		while (iterator.hasNext()) {
			T next = iterator.next();
			Order annotation = next.getClass().getAnnotation(Order.class);
			if (annotation == null) {
				iterator.remove();
			}
		}
		if (clazzList.size() > 1) {
			sort(clazzList);
		}
		return clazzList.get((clazzList.size() - 1) > 0 ? clazzList.size() - 1 : 0);
	}

	private static <T> void sort(List<T> list) {
		list.sort(new Comparator<T>() {
			@Override
			public int compare(T o1, T o2) {

				return o1.getClass().getAnnotation(Order.class).value()
						- o2.getClass().getAnnotation(Order.class).value();
			}
		});
	}
}
