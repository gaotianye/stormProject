package cn.celloud.bf.stormProject1.utils;

public class SleepUtils {
	public static void sleep(long time){
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
