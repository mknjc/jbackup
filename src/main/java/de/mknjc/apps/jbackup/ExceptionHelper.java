package de.mknjc.apps.jbackup;

public class ExceptionHelper {

	public static <R, T> R runtime(final ExceptionFunction<T, R> func, final T t) {
		try {
			return func.apply(t);
		} catch(final Exception e) {
			if(e instanceof RuntimeException) {
				throw (RuntimeException)e;
			} else {
				throw new RuntimeException(e);
			}
		}
	}

}
