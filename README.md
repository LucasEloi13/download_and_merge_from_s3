# Download e Consolidação de JSONs do S3

Script para baixar arquivos JSON de um bucket S3, salvar os arquivos brutos em `raw_files/` e gerar um arquivo final consolidado em **um único array JSON**.

## Como funciona

1. Carrega configurações (`.env` + argumentos CLI).
2. Lista automaticamente as keys no S3 com base em uma ou mais URIs completas `s3://bucket/prefixo/`.
3. Baixa os arquivos encontrados para `raw_files/`.
4. Faz merge em streaming e escreve um único output no formato:

```json
[
	{"...": "..."},
	{"...": "..."}
]
```

## Estrutura principal

- `main.py`
	- Orquestra o fluxo completo: descoberta de keys, download, merge e CLI.
	- Faz parse em streaming para evitar carregar todos os arquivos em RAM.
- `source/s3.py`
	- Conexão com S3 (`boto3`).
	- Listagem de keys (`list_keys`).
	- Download com barra de progresso (`download_object_to_file`).
	- Trata `NoSuchKey` como `FileNotFoundError` (arquivo ausente é pulado).

## Requisitos

- Python 3.10+
- Dependências:
	- `boto3`
	- `python-dotenv`

## Configuração via `.env`

Exemplo:

```env
AWS_PROFILE=seu-profile
AWS_S3_URIS=s3://nome-do-bucket/caminho/do/prefixo/,s3://outro-bucket/outro/prefixo/
```

> `AWS_PROFILE` é opcional (usa credenciais padrão se não informado).

## Uso

### Rodar com variáveis do `.env`

```bash
python3 main.py --input-format json_array
```

### Rodar informando URI S3 na linha de comando

```bash
python3 main.py \
	--s3-uri s3://prod-octaprice-crawl-input-output/parallel_pdp_outputs/165/20494_matched/results/ \
	--input-format json_array
```

### Rodar com 2 ou mais URIs

```bash
python3 main.py \
	--s3-uri \
	s3://bucket-1/prefixo-1/ \
	s3://bucket-2/prefixo-2/ \
	--input-format json_array
```

### Teste rápido com limite

```bash
python3 main.py --input-format json_array --limit 2
```

## Parâmetros CLI

- `--s3-uri`: uma ou mais URIs S3 completas. Pode repetir o argumento ou passar várias URIs de uma vez.
- `--output`: arquivo consolidado de saída. Padrão: `output/YYYYMMDD_HHMMSS.json`.
- `--input-format`: formato dos arquivos de entrada:
	- `json_array`: arquivos no formato `[{...}, {...}]`.
	- `json_objects`: objetos soltos (ex.: um por linha).
- `--file-suffix`: filtra keys por sufixo (padrão `.json`). Use `""` para não filtrar.
- `--raw-dir`: pasta dos arquivos baixados (padrão `raw_files`).
- `--limit`: limita quantidade de keys processadas.
- `--no-progress`: desativa barra de progresso de download.

## Comportamento de erro

- Se uma key não existir (`NoSuchKey`), o script **não para**:
	- loga aviso,
	- pula a key,
	- continua o processamento.
- Se nenhuma key for baixada com sucesso, o merge é abortado com warning.

## Observações importantes

- O output final é sempre um array JSON único.
- Quando duas URIs tiverem arquivos com o mesmo nome, o script salva os brutos com nome alternativo para evitar sobrescrita.
- O script processa em streaming para reduzir uso de memória.
- Caso rode fora de ambiente virtual, garanta que as dependências estejam instaladas no `python` usado.

## Exemplo de saída de pastas

- `raw_files/`: arquivos brutos baixados do S3.
- `output/YYYYMMDD_HHMMSS.json` (ou arquivo definido em `--output`): consolidado final.
