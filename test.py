import base64
from striprtf.striprtf import rtf_to_text

# Seu conteúdo base64 dentro de CDATA
encoded_content = """
NTU=
"""

# Corrigir o padding base64
padding = len(encoded_content) % 4
if padding != 0:
    encoded_content += '=' * (4 - padding)

# Decodificando o conteúdo base64
decoded_content = base64.b64decode(encoded_content).decode('latin1')
clean_text = rtf_to_text(decoded_content)


print(clean_text)
