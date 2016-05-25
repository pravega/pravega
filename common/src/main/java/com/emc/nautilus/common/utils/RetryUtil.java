package com.emc.nautilus.common.utils;

public class RetryUtil {

	public static interface Task<RetryableException extends Exception> {
		void attempt() throws RetryableException;
	}

	public static interface RecoverableTask<RetryableException extends Exception> 
			extends Task<RetryableException> {
		void recover();
	}

	public static <ExceptionType extends Exception> void retryWithBackoff(Task<ExceptionType> task) {
		return;// TODO
	}

}
