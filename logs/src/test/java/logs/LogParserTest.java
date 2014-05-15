package logs;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.purbon.hadoop.logs.parser.LogParser;

public class LogParserTest {

	LogParser parser;
	
	@Before
	public void setUp() throws Exception {
		parser = new LogParser();
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testParseDateTime() {
		String dateLine = "01/Aug/1995:00:01:13 -0400";
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
		boolean raised = false;
		try {
			sdf.parse(dateLine);
			raised = false;
		} catch (Exception ex) {
			raised = true;
		}
		assertFalse(raised);
	}

	@Test
	public void testPartialMatch() {
		String line = "133.68.18.180 - - [01/Aug/1995:00:01:13 -0400] \"GET /persons/nasa-cm/jmd-sm.gif HTTP/1.0\" 200 3660";
		Pattern pattern = Pattern.compile("([\\w|\\W&&[^\\s]]+)\\s\\-\\s\\-\\s\\[([\\w|\\W&&[^\\]]]+)\\]\\s\"(\\w+)\\s([\\w|\\W&&[^\\s]]+)\\s([\\w|\\W&&[^\"]]+)\"\\s(\\d+)\\s(\\d+)");
		Matcher m = pattern.matcher(line);
		assertTrue(m.matches());
	}
	
	@Test
	public void testValidLine() {

		String line = "133.68.18.180 - - [01/Aug/1995:00:01:13 -0400] \"GET /persons/nasa-cm/jmd-sm.gif HTTP/1.0\" 200 3660";
		Map<String, String> props = parser.parse(line);
		
		assertEquals("133.68.18.180", props.get(LogParser.HOST));
		assertEquals("01/Aug/1995:00:01:13 -0400", props.get(LogParser.DATETIME));
		assertEquals("GET", props.get(LogParser.METHOD));
		assertEquals("/persons/nasa-cm/jmd-sm.gif", props.get(LogParser.URL));
		assertEquals("HTTP/1.0", props.get(LogParser.PROTOCOL));
		assertEquals("200", props.get(LogParser.CODE));
		assertEquals("3660", props.get(LogParser.TIME));
		
	}

}
