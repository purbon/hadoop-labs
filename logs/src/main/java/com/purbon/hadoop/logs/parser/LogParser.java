package com.purbon.hadoop.logs.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser {
	
	public static final String HOST = "host";
	public static final String DATETIME = "datetime";
	public static final String METHOD = "method";
	public static final String URL = "url";
	public static final String PROTOCOL = "protocol";
	public static final String CODE = "code";
	public static final String TIME = "time";
	
	// uplherc.upl.com - - [01/Aug/1995:00:01:13 -0400] "GET /shuttle/resources/orbiters/challenger.html HTTP/1.0" 200 8089
	// 133.68.18.180 - - [01/Aug/1995:00:01:13 -0400] "GET /persons/nasa-cm/jmd-sm.gif HTTP/1.0" 200 3660
	
	private Pattern pattern = Pattern.compile("([\\w|\\W&&[^\\s]]+)\\s\\-\\s\\-\\s\\[([\\w|\\W&&[^\\]]]+)\\]\\s\"(\\w+)\\s([\\w|\\W&&[^\\s]]+)\\s([\\w|\\W&&[^\"]]+)\"\\s(\\d+)\\s(\\d+)");
	
	public Map<String, String> parse(String line) {
		Map<String, String> props = new HashMap<String, String>();
		
		Matcher m = pattern.matcher(line);
		if (m.matches()) {
			props.put(HOST, m.group(1));
			props.put(DATETIME, m.group(2));
			props.put(METHOD, m.group(3));
			props.put(URL, m.group(4));
			props.put(PROTOCOL, m.group(5));
			props.put(CODE, m.group(6));
			props.put(TIME, m.group(7));
		}
		return props;
		
	}
}
