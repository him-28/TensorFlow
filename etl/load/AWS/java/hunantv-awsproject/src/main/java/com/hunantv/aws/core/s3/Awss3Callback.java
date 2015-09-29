package com.hunantv.aws.core.s3;

import java.util.Map;

public interface Awss3Callback {
	public void notify(Map<String, Object> infomation);
}
