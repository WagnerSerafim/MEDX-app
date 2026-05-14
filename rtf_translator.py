import json
import re
import urllib.parse
import urllib.request


RTF_HARDCODED = r"""{\rtf1\ansi\deff0\deflang1033 {\fonttbl
{\f0\fswiss\fcharset0 ARIAL;}
}

{\colortbl
\red0\green0\blue0;\red0\green0\blue128;\red0\green128\blue0;\red0\green128\blue128;
\red128\green0\blue0;\red128\green0\blue128;\red128\green128\blue0;\red192\green192\blue192;
\red128\green128\blue128;\red0\green0\blue255;\red0\green255\blue0;\red0\green255\blue255;
\red255\green0\blue0;\red255\green0\blue255;\red255\green255\blue0;\red255\green255\blue255;
}
\paperw11175 \margr0\margl0\margt0\ATXph0 \plain \fs20 01/10/03-cisto uretral.\par
USG em 07/11/02: utero com 6,2x3,4x5,2    OD:50 cm devido a cisto.Usou Diane\par
cd-Orient.}"""


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
