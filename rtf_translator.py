import json
import re
import urllib.parse
import urllib.request


RTF_HARDCODED = r"""{\rtf1\ansi\deff0\deftab254{\fonttbl{\f0\fnil\fcharset0 Courier New;}{\f1\fnil\fcharset0 Arial;}}{\colortbl\red0\green0\blue0;\red255\green0\blue0;\red0\green128\blue0;\red0\green0\blue255;\red255\green255\blue0;\red255\green0\blue255;\red128\green0\blue128;\red128\green0\blue0;\red0\green255\blue0;\red0\green255\blue255;\red0\green128\blue128;\red0\green0\blue128;\red255\green255\blue255;\red192\green192\blue192;\red128\green128\blue128;\red255\green255\blue255;}\paperw12240\paperh15840\margl1880\margr1880\margt1440\margb1440{\*\pnseclvl1\pnucrm\pnstart1\pnhang\pnindent720{\pntxtb}{\pntxta{.}}}
{\*\pnseclvl2\pnucltr\pnstart1\pnhang\pnindent720{\pntxtb}{\pntxta{.}}}
{\*\pnseclvl3\pndec\pnstart1\pnhang\pnindent720{\pntxtb}{\pntxta{.}}}
{\*\pnseclvl4\pnlcltr\pnstart1\pnhang\pnindent720{\pntxtb}{\pntxta{)}}}
{\*\pnseclvl5\pndec\pnstart1\pnhang\pnindent720{\pntxtb{(}}{\pntxta{)}}}
{\*\pnseclvl6\pnlcltr\pnstart1\pnhang\pnindent720{\pntxtb{(}}{\pntxta{)}}}
{\*\pnseclvl7\pnlcrm\pnstart1\pnhang\pnindent720{\pntxtb{(}}{\pntxta{)}}}
{\*\pnseclvl8\pnlcltr\pnstart1\pnhang\pnindent720{\pntxtb{(}}{\pntxta{)}}}
{\*\pnseclvl9\pndec\pnstart1\pnhang\pnindent720{\pntxtb{(}}{\pntxta{)}}}
{\pard\ql\li0\fi0\ri0\sb0\sl\sa0 \plain\f0\fs28\cf0\b 2,5 HEXANODIONA URIN\'c1RIO\par
\ql\li0\fi0\ri0\sb0\sl\sa0 \plain\f0\fs24\cf0\b Material: urina - amostra isolada\par
\ql\li0\fi0\ri0\sb0\sl\sa0 \plain\f0\fs24\cf0\b M\'e9todo: Cromatografia gasosa\par
\ql\li0\fi0\ri0\sb0\sl\sa0 \plain\f0\fs24\cf0\b \par
\ql\li0\fi0\ri0\sb0\sl\sa0 \plain\f0\fs20\cf0 Resultado.\par
\ql\li0\fi0\ri0\sb0\sl\sa0 \plain\f1\fs24\cf0 \par
\ql\li0\fi0\ri0\sb0\sl\sa0 \plain\f0\fs24\cf0 VALORES DE REFER\'caNCIA\par
\ql\li0\fi0\ri0\sb0\sl\sa0 \plain\f0\fs20\cf0 N\'e3o detect\'e1vel\par
\ql\li0\fi0\ri0\sb0\sl\sa0 \plain\f1\fs20\cf0 }
}"""


def rtf_to_text(rtf_content: str) -> str:
	text = rtf_content
	text = re.sub(r"\\par[d]?", "\n", text)
	text = re.sub(r"\\'[0-9a-fA-F]{2}", "", text)
	text = re.sub(r"\\[a-zA-Z]+-?\d* ?", "", text)
	text = text.replace("{", "").replace("}", "")
	text = re.sub(r"\n+", "\n", text)
	return text.strip()


def translate_text(text: str, source: str = "pt", target: str = "en") -> str:
	query = urllib.parse.urlencode(
		{
			"q": text,
			"langpair": f"{source}|{target}",
		}
	)
	url = f"https://api.mymemory.translated.net/get?{query}"

	with urllib.request.urlopen(url, timeout=30) as response:
		payload = json.loads(response.read().decode("utf-8"))

	return payload["responseData"]["translatedText"]


def main():
	original_text = rtf_to_text(RTF_HARDCODED)
	translated_text = translate_text(original_text, source="pt", target="en")

	print("=== TEXTO EXTRAÍDO DO RTF ===")
	print(original_text)
	print("\n=== TEXTO TRADUZIDO ===")
	print(translated_text)


if __name__ == "__main__":
	main()
