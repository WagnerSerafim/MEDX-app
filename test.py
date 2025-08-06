import urllib.parse
import html

# Texto codificado
texto_codificado = """<div style=""text-align:center""><span style=""font-size:12px""><span style=""font-family:times new roman,times,serif""><strong>Atestado M&eacute;dico&nbsp;</strong></span></span></div>
<br />
<br />
<span style=""font-size:12px""><span style=""font-family:times new roman,times,serif"">Atesto que&nbsp;Carlos Alfredo.<br />
<br />
<br />
[ &nbsp;] Consulta.<br />
[ &nbsp;] Procedimento<br />
[ &nbsp;] __________________________________________________________.<br />
<br />
<br />
Informo que:<br />
[ &nbsp;] Pode voltar em seguida ao trabalho.<br />
[ &nbsp;] Foi encaminhado (a) para exames complementares.<br />
[ &nbsp;] Dever&aacute; ser afastado (a) do trabalho no dia de hoje das _____ as _____ horas.<br />
[ &nbsp;] Dever&aacute; permanecer afastado (a) no dia de hoje.<br />
[ &nbsp;] Dever&aacute; permanecer afastado (a) por _____ dia (s), a contar do dia _____ at&eacute; o dia _____.&nbsp;<br />
[ &nbsp;]&nbsp;___________________________________________________________.<br />
<br />
<br />
<br />
<br />
Autorizo a declara&ccedil;&atilde;o do CID &nbsp; &nbsp; ___________________________________<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;Assinatura do Paciente<br />
<br />
<br />
<strong>CID</strong>:</span></span><br />
&nbsp;
<div style=""text-align:right"">27/07/2021</div>
"""

# 1. Decodifica URL encoding
url_decodificado = urllib.parse.unquote_plus(texto_codificado)

# 2. Decodifica entidades HTML
html_decodificado = html.unescape(url_decodificado)

print(html_decodificado)

