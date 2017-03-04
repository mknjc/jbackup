package de.mknjc.apps.jbackup;

public interface ExceptionFunction<T, R> {
	R apply(T t) throws Exception;

}
