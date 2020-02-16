package com.taxonic.carml.util;

import java.util.regex.Pattern;

@SuppressWarnings({ "squid:S1192", "squid:S00115" })
public final class LangTagUtil {

	public enum Standard {
		BCP_47, RFC_3066
	}

	private LangTagUtil() {}

	private static final String ALPHA = "[A-Za-z]";

	private static final String DIGIT = "[0-9]";

	private static final String ALPHANUM = String.format("(?:%s|%s)", ALPHA, DIGIT);

	private static final String _2_ALPHA = String.format("(?:%s{2})", ALPHA);

	private static final String _3_ALPHA = String.format("(?:%s{3})", ALPHA);

	private static final String _4_ALPHA = String.format("(?:%s{4})", ALPHA);

	private static final String _1_8_ALPHA = String.format("(?:%s{1,8})", ALPHA);

	private static final String _2_3_ALPHA = String.format("(?:%s{2,3})", ALPHA);

	private static final String _5_8_ALPHA = String.format("(?:%s{5,8})", ALPHA);

	private static final String _3_DIGIT = String.format("(?:%s{3})", DIGIT);

	private static final String _3_ALPHANUM = String.format("(?:%s{3})", ALPHANUM);

	private static final String _1_8_ALPHANUM = String.format("(?:%s{1,8})", ALPHANUM);

	private static final String _2_8_ALPHANUM = String.format("(?:%s{2,8})", ALPHANUM);

	private static final String _5_8_ALPHANUM = String.format("(?:%s{5,8})", ALPHANUM);

	private static final String EXTLANG = String.format("(?:%s(?:-%s){0,2})", _3_ALPHA, _3_ALPHA);

	private static final String SINGLETON = String.format("(?:%s|[A-W]|[Y-Z]|[a-w]|[y-z])", DIGIT);

	private static final String IRREGULAR = "(?:en-GB-oed|i-ami|i-bnn|i-default|i-enochian|i-hak|i-klingon|i-lux|i-mingo|i-navajo|i-pwn|i-tao|i-tay|i-tsu|sgn-BE-FR|sgn-BE-NL|sgn-CH-DE)";

	private static final String REGULAR = "(?:art-lojban|cel-gaulish|no-bok|no-nyn|zh-guoyu|zh-hakka|zh-min|zh-min-nan|zh-xiang)";

	private static final String LANGUAGE = String.format("(?:(?:%s(?:-%s)?)|%s|%s)", _2_3_ALPHA, EXTLANG, _4_ALPHA,
			_5_8_ALPHA);

	private static final String SCRIPT = _4_ALPHA;

	private static final String REGION = String.format("(?:%s|%s)", _2_ALPHA, _3_DIGIT);

	private static final String VARIANT = String.format("(?:%s|%s%s)", _5_8_ALPHANUM, DIGIT, _3_ALPHANUM);

	private static final String EXTENSION = String.format("(?:%s(?:-%s)+)", SINGLETON, _2_8_ALPHANUM);

	private static final String PRIVATE_USE = String.format("(?:x(?:-%s)+)", _1_8_ALPHANUM);

	private static final String GRANDFATHERED = String.format("(?:%s|%s)", IRREGULAR, REGULAR);

	private static final String LANGTAG = String.format("(?:%s(?:-%s)?(?:-%s)?(?:-%s)*(?:-%s)*(?:-%s)?)",
			LANGUAGE, SCRIPT, REGION, VARIANT, EXTENSION, PRIVATE_USE);

	private static final String BCP_47_LANGUAGE_TAG = String.format("^(?:%s|%s|%s)$", LANGTAG, PRIVATE_USE,
			GRANDFATHERED);

	private static final Pattern BCP_47_LANGUAGE_TAG_PATTERN = Pattern.compile(BCP_47_LANGUAGE_TAG);

	private static final String RFC_3066_LANGUAGE_TAG = String.format("^(?:%s(?:-%s)*)$", _1_8_ALPHA, _1_8_ALPHANUM);

	private static final Pattern RFC_3066_LANGUAGE_TAG_PATTERN = Pattern.compile(RFC_3066_LANGUAGE_TAG);

	/**
	 * Checks whether the provided language tag is well-formed according to
	 * {@link Standard#BCP_47}. See <a href=
	 * "https://tools.ietf.org/html/bcp47#section-2.2.9">https://tools.ietf.org/html/bcp47#section-2.2.9</a>.
	 * 
	 * @param langTag the language tag to check
	 * @return
	 */
	public static boolean isWellFormed(String langTag) {
		return isWellFormed(langTag, Standard.BCP_47);
	}

	/**
	 * Checks whether the provided language tag is well-formed according to the
	 * specified {@link Standard}.
	 * 
	 * @param langTag  the language tag to check
	 * @param standard the standard to which langTag should conform
	 * @return
	 */
	public static boolean isWellFormed(String langTag, Standard standard) {
		switch (standard) {
		case BCP_47:
			return BCP_47_LANGUAGE_TAG_PATTERN.matcher(langTag).matches();
		case RFC_3066:
			return RFC_3066_LANGUAGE_TAG_PATTERN.matcher(langTag).matches();
		default:
			throw new IllegalStateException(String.format("No behaviour implemented for '%s'", standard));
		}
	}
}
