package engineering.taikun.stitch;

import jdk.nashorn.api.scripting.NashornScriptEngineFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

public final class Stitch {

	private static final Pattern NEWLINE_PATTERN = Pattern.compile("\\n");

	private Stitch() {}

	public static String evaluate(final String input, final Map<String, Object> values)
			throws ScriptException, NoSuchMethodException {

		final HashMap<String, Object> copy = new HashMap<>(values.size());
		for (final Entry<String, Object> entry : values.entrySet()) {
			final String new_key = entry.getKey().toUpperCase();

			if (copy.containsKey(new_key)) {
				throw new IllegalArgumentException("duplicate key");
			}

			copy.put(new_key, entry.getValue());
		}

		final StringBuilder sb = new StringBuilder();
		final String[] lines = NEWLINE_PATTERN.split(input);
		final boolean[] includes = new boolean[lines.length];

		for (final Entry<String, Object> entry : copy.entrySet()) {
			final String key = entry.getKey();
			final Object value = entry.getValue();

			sb.append(key).append('=');

			if (value instanceof String) {
				sb.append('"').append(value).append('"');
			} else  {
				sb.append(value);
			}

			sb.append(";\n");
		}

		sb.append("function a() { var lines = [];").append('\n');

		boolean keep_lines = false;
		for (int i = 0; i < lines.length; i++) {
			final String line = lines[i];
			final String trimmed = line.trim();

			if (trimmed.startsWith("//!")) {

				keep_lines = false;
				sb.append(trimmed.substring(3)).append('\n');

			} else if (trimmed.startsWith("//$")) {

				keep_lines = true;
				includes[i] = true;
				sb.append(trimmed.substring(3)).append('\n');

			} else {

				includes[i] = keep_lines;
				sb.append("lines.push(").append(i).append(");").append('\n');

			}
		}

		sb.append("return Java.to(lines, \"int[]\"); }");

		final ScriptEngine engine = new NashornScriptEngineFactory().getScriptEngine();
		engine.eval(sb.toString());

		final Invocable inv = (Invocable) engine;

		final int[] line_array = (int[]) inv.invokeFunction("a");

		final StringBuilder toreturn = new StringBuilder();

		int line_index = 0;
		for (int i = 0; i < lines.length; i++) {
			if (line_array[line_index] == i) {
				toreturn.append(lines[i]).append('\n');
				line_index++;
			} else if (includes[i]) {
				toreturn.append('\n');
			}
		}

		return toreturn.toString();

	}

}
