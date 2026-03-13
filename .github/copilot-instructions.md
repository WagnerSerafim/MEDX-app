# MEDX-app Copilot Instructions

## Contexto do repositório

Este projeto é um monorepo Python com múltiplas integrações de sistemas clínicos. Em geral, cada pasta de primeiro nível representa uma integração independente, com scripts de ETL para migrar pacientes, agendamentos, prontuários, receitas, diagnósticos e documentos.

Padrões recorrentes no repositório:

- Arquivos como `patients.py`, `schedule.py`, `record_*.py`, `records_*.py` e `indexes*.py` representam fluxos específicos de importação ou migração.
- A maior parte dos scripts executa leitura de origem, normalização de dados, inserção em banco e geração de logs.
- Há muita repetição entre integrações. Antes de editar um arquivo, compare com implementações irmãs na mesma pasta ou em integrações parecidas.

## Como analisar antes de editar

- Primeiro entenda o fluxo completo do arquivo: entrada de dados, transformação, escrita em banco e logging.
- Procure funções equivalentes em integrações vizinhas antes de propor refatorações ou mudanças de comportamento.
- Prefira mudanças locais e pequenas. Não faça refatorações amplas entre várias integrações sem pedido explícito.
- Preserve nomes de colunas, chaves de payload e contratos de saída exatamente como já estão, salvo quando a tarefa exigir mudança direta nisso.

## Convenções importantes deste projeto

- Preserve a ordem e o formato de inputs interativos quando já existirem.
- Preserve helpers de limpeza e normalização já usados no fluxo, especialmente utilitários como limpeza de CPF, telefone, datas, sexo, `verify_nan`, truncamento e normalização de strings.
- Quando um script trabalha com inserção em lote, mantenha o padrão existente de `BATCH_SIZE`, `commit`, `rollback` e contabilização de inseridos e não inseridos.
- Não remova logs estruturados de falha. Toda exceção ou descarte de registro deve continuar registrando motivo de forma rastreável.
- Mantenha compatibilidade com o estilo atual do arquivo. O código do repositório é majoritariamente procedural; não introduza abstrações grandes sem necessidade.

## Banco de dados e ETL

- Muitos scripts usam SQLAlchemy com leitura da origem e escrita no destino. Preserve esse fluxo.
- Não altere builders de conexão, schemas, nomes de tabela ou nomes de coluna sem confirmar no código existente da integração.
- Em migrações, priorize segurança de dados: validar campos nulos, sanitizar antes da escrita e truncar valores conforme limites já usados no destino.
- Ao lidar com IDs já existentes, mantenha a lógica atual de deduplicação antes de inserir.

## Logging e validação

- Preserve contadores de progresso, especialmente em processamentos longos.
- Preserve ou amplie logs em JSON ou JSONL quando a alteração afetar regras de inserção, descarte ou erro.
- Se não houver testes automatizados, valide de forma objetiva: execução do script alvo, checagem de erros de sintaxe e conferência de logs/resultados esperados.
- Ao modificar um script de uma integração, prefira validar apenas esse fluxo específico em vez de tentar padronizar todo o repositório.

## Diretrizes para mudanças do Copilot

- Para corrigir bugs, ataque a causa raiz e evite remendos superficiais.
- Evite alterar comportamento em múltiplas integrações ao mesmo tempo, a menos que isso seja explicitamente solicitado.
- Ao criar um novo script em uma integração, siga o padrão de nomes e estrutura já existente naquela pasta.
- Ao reutilizar lógica, prefira copiar o padrão comprovado de uma integração semelhante antes de inventar uma estrutura nova.
- Se houver dúvida sobre comportamento esperado, use a implementação análoga mais próxima como referência principal.

## O que evitar

- Não substituir fluxos procedurais existentes por arquitetura orientada a objetos sem necessidade clara.
- Não remover prints de progresso, tratamento de exceção ou geração de logs sem reposição equivalente.
- Não renomear arquivos recorrentes como `patients.py`, `schedule.py` ou `record_*.py` sem pedido explícito.
- Não presumir que duas integrações distintas aceitam exatamente o mesmo mapeamento de campos.

## Validação mínima após mudanças

- Verificar erros de sintaxe no arquivo alterado.
- Executar apenas o script impactado, quando viável.
- Confirmar que logs, contadores e payloads continuam coerentes com o padrão da integração.
- Em mudanças de mapeamento, revisar truncamento, campos obrigatórios e tratamento de nulos.