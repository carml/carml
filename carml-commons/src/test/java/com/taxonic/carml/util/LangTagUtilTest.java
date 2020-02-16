package com.taxonic.carml.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

import com.taxonic.carml.util.LangTagUtil.Standard;

public class LangTagUtilTest {

	@Test
	public void givenWellFormedLangTag_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("nl"), is(true));
	}

	@Test
	public void givenLongerWellFormedLangTag_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("english"), is(true));
	}

	@Test
	public void givenIllFormedLangTag_returnsIllFormed() {
		assertThat(LangTagUtil.isWellFormed("Nederlands"), is(false));
	}

	@Test
	public void givenLangTagWithScript_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("en-Latn"), is(true));
	}

	@Test
	public void givenLangTagWithIncorrectFollwer_returnsIllFormed() {
		assertThat(LangTagUtil.isWellFormed("en-L"), is(false));
	}

	@Test
	public void givenLangTagWithScriptAndRegion_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("mn-Cyrl-MN"), is(true));
	}

	@Test
	public void givenLangTagWithScriptAndIncorrectRegion_returnsIllFormed() {
		assertThat(LangTagUtil.isWellFormed("mn-Cyrl-M"), is(false));
	}

	@Test
	public void givenLangTagWithScriptRegionAndVariant_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("hy-Latn-IT-arevela"), is(true));
	}

	@Test
	public void givenLangTagWithScriptRegionAndIncorrectVariant_returnsIllFormed() {
		assertThat(LangTagUtil.isWellFormed("hy-Latn-IT-arev"), is(false));
	}

	@Test
	public void givenLangTagWithScriptRegionAndNumericVariant_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("hy-Latn-IT-1234"), is(true));
	}

	@Test
	public void givenLangTagWithScriptRegionAndIncorrectNumericVariant_returnsIllFormed() {
		assertThat(LangTagUtil.isWellFormed("hy-Latn-IT-123"), is(false));
	}

	@Test
	public void givenLangTagWithAllPossibleFollowers_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("en-latn-gb-boont-r-extended-sequence-x-private"), is(true));
	}

	@Test
	public void givenIrregularGranfatheredTag_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("i-klingon"), is(true));
	}

	@Test
	public void givenRegularGranfatheredTag_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("zh-xiang"), is(true));
	}

	@Test
	public void givenIncorrectGranfatheredTag_returnsIllFormed() {
		assertThat(LangTagUtil.isWellFormed("i-klingons"), is(false));
	}

	@Test
	public void givenWellFormedPrivateUseTag_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("x-whatever"), is(true));
	}

	@Test
	public void givenWellFormedRepeatingPrivateUseTag_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("x-whatever-eva-123-eva"), is(true));
	}

	@Test
	public void givenIllFormedPrivateUseTag_returnsIllFormed() {
		assertThat(LangTagUtil.isWellFormed("x-123456789"), is(false));
	}

	@Test
	public void givenLangTagWithAllPossibleFollowers_givenStandardBCP_47_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("en-latn-gb-boont-r-extended-sequence-x-private", Standard.BCP_47),
				is(true));
	}

	@Test
	public void givenLangTagWithAllPossibleFollowers_givenStandardRFC_3066_returnsWellFormed() {
		assertThat(LangTagUtil.isWellFormed("en-latn-gb-boont-r-extended-sequence-x-private", Standard.RFC_3066),
				is(true));
	}
}
