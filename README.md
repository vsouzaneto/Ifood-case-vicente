# Case iFood — Pipeline NYC Taxi (Databricks + PySpark)

Pipeline com arquitetura **medalhão** de 4 camadas sobre os dados públicos da NYC TLC
(yellow, green, fhv e fhvhv, jan–mai/2023), construído em **PySpark** no Databricks e
publicado no **Unity Catalog** (catálogo `ifood`) para consumo via SQL.

Este README resume o notebook inteiro: **resultados primeiro**, depois a arquitetura,
as decisões de modelagem e o passo a passo de cada seção. O relatório completo
(código, outputs e gráficos) está em **`case-ifood-notebook.pdf`**.

---

## Resultados

### Pergunta 1 — média de `total_amount` por mês (yellow)

A pergunta admite duas leituras; as duas são respondidas com SQL sobre a gold
(`gold.fct_taxi_trips_yellow_green`, o fato leve):

1. **Ticket médio por corrida** (leitura principal): `AVG(total_amount)` por mês;
2. **Receita média mensal** (leitura alternativa): média das 5 somas mensais.

| Mês | Corridas | Ticket médio (US$) | Receita do mês (US$) |
| --- | --- | --- | --- |
| 2023-01 | 2.948.546 | 27,41 | 80.821.977,02 |
| 2023-02 | 2.804.468 | 27,32 | 76.607.284,01 |
| 2023-03 | 3.271.381 | 28,25 | 92.430.477,76 |
| 2023-04 | 3.165.284 | 28,73 | 90.935.817,69 |
| 2023-05 | 3.380.075 | 29,44 | 99.522.369,27 |

**Resposta:** o ticket médio sobe de **27,41** para **29,44** dólares (**+7,4% em cinco
meses**), com leve recuo em fevereiro e alta contínua depois. Leitura alternativa:
receita média mensal de **US$ 88.063.585,15**.

### Pergunta 2 — média de `passenger_count` por hora (maio/2023)

**Premissa:** corridas com 1–6 passageiros (`is_valid_passenger_count`). Fhv/fhvhv não
possuem o campo, então restam yellow+green — "todos os táxis da frota" que têm
`passenger_count`. Os nulos **não** entram na média (e nunca como 0): o filtro os exclui
antes do `AVG`; em maio são 3.445.084 linhas no mês, 3.342.318 entram na média e 102.766
são nulas — se valessem 0, a média cairia de 1,382 para 1,341 (~3% de subestimação).

**Resposta:** média entre **1,263** (6h) e **1,455** (2h) passageiros — pico na madrugada
(2–3 h, ~1,45–1,46) e vale na manhã (5–6 h, ~1,26–1,28). Corridas noturnas tendem a
carregar grupos (bares, aeroporto); a manhã é dominada por deslocamentos individuais de
trabalho. O gráfico de linha (X = hora, Y = média) está no notebook, na célula da
pergunta 2, e a série completa por hora em `gold.agg_avg_passengers_by_hour`.

### Qualidade dos dados (EDA)

* **Bronze: 118.557.800 linhas → Gold: 117.899.102 válidas** (0,56% descartado)
* **Quarentena auditável: 652.856 linhas** com motivo por linha — yellow 616.630,
  green 19.145, fhv 1, fhvhv 17.080. Principais motivos: `passenger_count` fora de 1–6
  (269.557), `trip_distance` não positiva (208.269), `total_amount` não positivo
  (125.844), `trip_miles` não positiva (17.068)
* **Dedup: 5.842 duplicatas exatas** removidas, com chaves semânticas por tipo

### Robustez dos resultados (EDA extra)

**1. Sensibilidade da pergunta 1** — recálculo direto na bronze (sem limpeza) vs gold
(limpada): a limpeza **corrige um viés de subestimação** causado por tarifas
zeradas/negativas (~1,5%):

| Mês | Bronze (sem limpeza) | Gold (limpa) | Ganho da limpeza |
| --- | --- | --- | --- |
| 2023-01 | 27,02 | 27,41 | +0,39 |
| 2023-02 | 26,90 | 27,32 | +0,42 |
| 2023-03 | 27,80 | 28,25 | +0,45 |
| 2023-04 | 28,27 | 28,73 | +0,46 |
| 2023-05 | 28,96 | 29,44 | +0,48 |

**2. Datas fora do mês do arquivo** — valida a regra que protege o particionamento:
yellow 437 linhas (0,0027%), green 48 (0,014%), fhv e fhvhv **zero** — `pickup_year_month`
é confiável como partição e eixo temporal.

**3. Outliers e percentis (yellow)** — a limpeza **não corta outliers arbitrariamente**:
p50 US$ 20,79, p90 US$ 58,30, p99 US$ 102,66; distância p99 20,2 milhas. Remover o 1%
mais caro mudaria a média de 28,28 para 27,30 (−3,5%) — corte **não** aplicado: o único
corte é o inválido (tarifa ≤ 0), a cauda longa legítima (aeroporto, corridas longas)
permanece.

### Conclusões

1. **Pergunta 1 — ticket médio em alta constante**: 27,41 → 29,44 (+7,4%), leve recuo
   em fevereiro; receita média mensal de 88,06 milhões.
2. **Pergunta 2 — madrugada carrega mais passageiros**: média por hora entre 1,26 e
   1,46, pico na madrugada e vale na manhã — padrão claro no gráfico.
3. **Qualidade com rastro auditável**: 652.856 rejeitadas (0,56%), todas com motivo
   consultável por SQL na quarentena; nada descartado silenciosamente.
4. **A limpeza corrige viés real**: sem a silver, o ticket médio sairia 0,39–0,48 dólares
   menor por mês — as respostas virariam números subestimados.
5. **Por que confiar**: dedup semântica (5.842 duplicatas), pipeline idempotente (Run all
   reproduz do zero), perguntas calculadas por SQL independente e conferidas contra os
   agregados materializados, e as colunas obrigatórias do case verificadas por `assert`.

---

## Arquitetura medalhão

| Camada | Papel | Saída |
| --- | --- | --- |
| Landing | Arquivos originais para auditoria e reprocesso | Volume `landing.raw_files` + `landing.controle_ingestao` |
| Bronze | Padroniza tipos e nomes sem perder linhas + linhagem do arquivo | `bronze.yellow/green/fhv/fhvhv_tripdata` |
| Silver | Qualidade por natureza, sem misturar tipos; inválidos em quarentena com motivo | `silver.<tipo>_trips` + `<tipo>_trips_rejected` |
| Gold | Unifica os 4 tipos (panorama geral) com clustering líquido; fato leve só com yellow+green | `gold.fct_taxi_trips`, `gold.fct_taxi_trips_yellow_green`, `gold.vw_taxi_trips`, `gold.agg_*` |

20 arquivos `.parquet` originais (4 tipos × jan–mai/2023, ~2,4 GB) ficam no Volume
`landing.raw_files`; os dicionários oficiais da TLC em PDF estão catalogados na
seção 1b. Fonte: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

### Os 4 tipos de táxi

* **Yellow** — táxis amarelos de Manhattan: passageiro na rua/ponto, tarifa por **medidor**
  (`total_amount`, `trip_distance`, `passenger_count`, `VendorID`);
* **Green** (Boro Taxi) — táxis verdes dos bairros fora do centro (onde o yellow não pode
  pegar na rua), mesmo schema de tarifas do yellow;
* **FHV** — veículos por chamada/app, **sem medidor**: origem, destino e horários, mas sem
  tarifa nem passageiros medidos;
* **FHVHV** — apps de alto volume (Uber, Lyft, Via), ~81% das linhas, com campos próprios
  (`trip_miles`, `trip_time`, `driver_pay`, `dispatching_base_num`).

> É essa heterogeneidade que justifica o schema canônico **por tipo** na bronze e a
> unificação `unionByName` na gold: só yellow/green têm os campos medidos por tarifa
> (perguntas 1 e 2), enquanto fhv/fhvhv entram no fato para dar o panorama completo da
> mobilidade em NY.

### Decisões de modelagem

* **Silver por tipo**: cada natureza é processada de forma independente, com regras
  próprias (comuns: datas; medidor: yellow/green; despacho: fhvhv) e quarentena
  auditável por motivo. A unificação acontece na **gold**.
* **Colunas canônicas**: `tpep_/lpep_/Pickup_` viram `pickup_datetime`/`dropoff_datetime`
  — um nome único para os 4 tipos; `VendorID` vira `vendor_id`.
* **Deduplicação por tipo**: yellow/green pela tarifa medida; fhv/fhvhv pela base de
  despacho (+ `driver_pay` no fhvhv) — sem a base, corridas de bases diferentes seriam
  colapsadas indevidamente.
* **Clustering líquido na gold**: `fct_taxi_trips` e o fato leve usam
  `CLUSTER BY (pickup_year_month, taxi_type)` — pruning por mês **e** por tipo. No Delta,
  `PARTITIONED BY` e `CLUSTER BY` são mutuamente exclusivos, e BOOLEAN não suporta data
  skipping (`is_valid_passenger_count` não pode ser chave).
* **Fato leve de consumo**: `fct_taxi_trips_yellow_green` — só os táxis com medidor
  (15.890.239 corridas, 0,3 GB contra 3,25 GB do fato completo); fonte das perguntas 1
  e 2 e dos agregados (resultados idênticos ao fato completo, validado).
* **Nulo ≠ inválido**: a silver só rejeita valores **errados**; valores **ausentes**
  passam e a exclusão fica explícita na análise (flag `is_valid_passenger_count`).
* **Idempotência**: bronze grava por partição com `replaceWhere`; schemas com
  `IF NOT EXISTS` — o notebook pode ser reprocessado do zero (Run all).
* **Escopo automático**: o setup descobre tipos, ano e meses pelos nomes dos parquets
  (`<tipo>_tripdata_<ano>-<mes>.parquet`); o catálogo de destino vem do widget `catalog`.
* **Dicionários catalogados**: PDFs da TLC parseados com `ai_parse_document`/`ai_extract`
  viram `landing.dicionario_dados`, consultável por SQL.
* **Governança bilíngue**: 20 tabelas/views com descrição e 738 colunas com comentário
  `PT: ... | EN: ...` no Catalog Explorer e no `information_schema`.

---

## O pipeline, seção a seção (do notebook)

### 0. Setup — catálogo, schemas e Volume

Cria os schemas `landing`, `bronze`, `silver`, `gold` e o Volume `landing.raw_files` no
catálogo do widget `catalog` — tudo `IF NOT EXISTS` (idempotente, pré-requisito do Run all).
O escopo (tipos, ano, meses) é **descoberto** pelos nomes dos parquets no volume.

### 1. Landing — organização, validação e logs

Copia os uploads para `nyc_tlc/<tipo>/` (originais intactos para auditoria), confere os
20 arquivos esperados e valida a integridade de cada um (o `count()` força a leitura
completa do parquet). Grava `landing.controle_ingestao` com status (`ok`/`missing`/
`invalid`), tamanho, nº de linhas e `ingested_at` por arquivo — e **falha listando os
arquivos** se algo faltar ou quebrar.

### 1b. Dicionários de dados no catálogo

Os PDFs oficiais da TLC viram duas tabelas: `landing.dicionario_documentos` (binário +
parse estruturado em VARIANT via `ai_parse_document`) e `landing.dicionario_dados`
(uma linha por campo, via `ai_extract` — **72 campos**: fhv 7, green 20, fhvhv 25,
yellow 20), consultável por SQL sem abrir PDF.

### 2. Bronze — schema fixo, linhagem e `replaceWhere`

Os parquets da TLC mudam tipos (INT32/INT64/DOUBLE) e grafia (`Airport_fee` ×
`airport_fee`) entre os meses — então a bronze lê **arquivo a arquivo**, normaliza para
o schema unificado (41 colunas) e adiciona linhagem (`taxi_type`, `pickup_year_month`,
`_source_file`, `_ingested_at`). Horários ficam `TIMESTAMP_NTZ` (hora local de NY, sem
deslocamento pela sessão UTC); cada mês é gravado com `replaceWhere` na partição
`pickup_year_month` — reprocessar um mês substitui só aquele mês.

**Resultado: 118.557.800 linhas** (yellow 16,19 M; green 339,6 k; fhv 6,19 M;
fhvhv 95,85 M), nenhuma linha perdida.

### 3. Silver — qualidade, quarentena e deduplicação (por tipo)

Cada tipo é processado e gravado de forma independente (`silver.<tipo>_trips` +
quarentena `<tipo>_trips_rejected` com `rejection_reason` e `rejected_at`). Colunas
derivadas: `pickup_hour`, `trip_duration_min`, `is_valid_passenger_count`. Válidas:
**117.899.102 linhas**; quarentena: **652.856 (0,56%)**.

### 3b. Regras de validação (e por que essas)

Toda regra violada gera um motivo na linha (concatenados com `"; "` quando há mais de
um). Regras **explicáveis pelo negócio**, nenhum corte estatístico arbitrário:

| # | Regra | Válida quando | Linhas |
| --- | --- | --- | --- |
| 1 | dropoff ≤ pickup (todos) | `dropoff > pickup` | 1.180 |
| 2 | corrida fora do mês do arquivo (todos) | mês do pickup = mês do arquivo | 390 |
| 3 | `passenger_count` fora de 1–6 (yellow/green) | `BETWEEN 1 AND 6` | 269.557 |
| 4 | `total_amount` ≤ 0 (yellow/green) | `> 0` | 125.844 |
| 5 | `trip_distance` ≤ 0 (yellow/green) | `> 0` | 208.269 |
| 6 | `trip_miles` ≤ 0 (fhvhv) | `> 0` | 17.068 |
| 7 | `trip_time` ≤ 0 (fhvhv) | `> 0` | 3 |

Fhv não tem regra própria (sem medidor): só as duas comuns — **1 única linha** de fhv
na quarentena. Motivos predominantes são registros de fechamento de caixa/teste de
sistema (0 ou >6 passageiros, tarifas zeradas), não corridas reais.

### Nulo não vai para a quarentena (decisão de design)

`NULL BETWEEN 1 AND 6` avalia para `NULL` (não falso): sem motivo gerado, a linha é
válida. **Nulo é informação ausente, não valor inválido** — são 451.561 corridas de
yellow/green com tarifa, distância e horários válidos (424.274 delas seguem na silver).
A exclusão acontece na análise, com a flag `is_valid_passenger_count` — e nunca como 0.
Em fhv/fhvhv o campo nem existe por natureza; rejeitar nulo mandaria 102 milhões de
corridas para a quarentena.

### 4. Gold — camada de consumo (fato, view e agregados)

1. `gold.fct_taxi_trips`: panorama geral — `unionByName` das 4 silvers, clustering
   líquido (`pickup_year_month`, `taxi_type`), 48 colunas;
2. `gold.fct_taxi_trips_yellow_green`: fato leve (só medidor) — 15.890.239 linhas,
   0,3 GB, mesma clustering; fonte das perguntas 1 e 2;
3. `gold.vw_taxi_trips`: view de consumo para o usuário final;
4. `gold.agg_avg_total_amount_by_month` e `gold.agg_avg_passengers_by_hour`:
   agregados das duas perguntas, materializados;
5. `assert` confere as **5 colunas obrigatórias** do case (`vendor_id`,
   `passenger_count`, `total_amount`, `pickup_datetime`, `dropoff_datetime`) no fato
   **e** no fato leve.

### 5. Análises — as 2 perguntas

Respondidas por SQL sobre a gold (código em `analysis/01` e `02`), com os agregados
materializados batendo com as queries — resultados na seção **Resultados** acima.

### EDA extra — robustez

Sensibilidade bronze × gold, datas fora do mês e percentis/outliers — seção
**Robustez** acima (`analysis/03` + células SQL do notebook).

### 6. Governança — descrições bilíngues (PT + EN)

**20 tabelas/views** com descrição e **738 colunas** com comentário `PT: ... | EN: ...`,
aplicadas com `ALTER TABLE ... SET TBLPROPERTIES` / `ALTER COLUMN ... COMMENT` (view:
recriada com colunas comentadas + `COMMENT ON VIEW` depois). A célula é **idempotente e
incremental**: só aplica o que falta.

---

## Onde ver no Unity Catalog

* **Camadas**: catálogo `ifood` — os schemas `landing`, `bronze`, `silver` e `gold`
  **são** as camadas (Catalog Explorer);
* **Tabelas**: aba *Tables* de cada schema, com *Columns* (comentários bilíngues),
  *Sample data* e *Permissions*;
* **Volume dos originais**: `landing` → *Volumes* → `raw_files`;
* **Linhagem**: aba *Lineage* — silvers → fato → view/agregados, navegável até a bronze;
* **Time travel**: aba *History* — cada reprocesso versionado e restaurável
  (a prova da idempotência);
* **Consumo SQL**: menu *New → Query*: `SELECT * FROM ifood.gold.vw_taxi_trips`.

## Atendimento aos requisitos do case

| Requisito | Onde |
| --- | --- |
| Ingerir corridas de táxi de NY, jan–mai/2023 | Seções 0–2 (landing valida os 20 parquets, bronze padroniza) |
| Armazenar os originais em landing zone | Volume `landing.raw_files` + `landing.controle_ingestao` |
| Manipular/limpar os dados julgado necessário | Silver: regras por natureza, quarentena auditável, dedup |
| Colunas obrigatórias na camada de consumo | `fct_taxi_trips` **e** fato leve (48 colunas), conferidas por `assert` (seção 4) |
| Criação de tabelas — modelar e criar | Seção 0 (schemas idempotentes) + seções 1–4 |
| Disponibilizar para consumo via SQL | `vw_taxi_trips` + fato leve + agregados `agg_*` |
| Usar PySpark | Todo o pipeline (SQL embutido onde conveniente) |
| Responder às 2 perguntas | Seção 5 + agregados materializados na gold |
| EDA | Volume por camada, % descartado, motivos de rejeição, sensibilidade e outliers |

## Estrutura do repositório

```
ifood-case/
├── src/                      # pipeline PySpark (exportado do notebook, em ordem)
│   ├── 00_setup.py           # catálogo (widget), schemas, volume, escopo automático
│   ├── 01_landing.py         # organização, validação e controle de ingestão dos 20 parquets
│   ├── 02_dicionarios.py     # dicionários TLC no catálogo (ai_parse_document/ai_extract)
│   ├── 03_bronze.py          # schema canônico por tipo, linhagem, replaceWhere
│   ├── 04_silver.py          # qualidade por natureza, quarentena, dedup por tipo
│   └── 05_gold.py            # panorama (unionByName + clustering), fato leve, view e agregados
├── analysis/                 # queries SQL das análises (executáveis num SQL warehouse)
│   ├── 01_pergunta1_total_amount_por_mes.sql
│   ├── 02_pergunta2_passageiros_por_hora.sql
│   └── 03_eda_resumo.sql
├── README.md
├── case-ifood-notebook.pdf   # relatório completo do notebook (código, decisões e respostas)
└── requirements.txt          # instruções de acesso e execução
```

O notebook completo (com documentação passo a passo, validações e o gráfico da
pergunta 2) fica no workspace Databricks: **Case iFood — Pipeline NYC Taxi (Medalhão)**.
O **`case-ifood-notebook.pdf`** é o relatório desse notebook — capa com os
números-chave, sumário, código e outputs — gerado pela Seção 7 do notebook
(escritor de PDF puro, sem dependências externas; regenerável rodando a célula).

## Como consumir

Qualquer usuário com acesso ao catálogo `ifood` consulta por SQL:

* Fato completo: `SELECT * FROM ifood.gold.fct_taxi_trips WHERE taxi_type = 'yellow'`
* Fato leve (só yellow e green — consultas de tarifa e passageiros):
  `SELECT * FROM ifood.gold.fct_taxi_trips_yellow_green`
* View de consumo: `SELECT * FROM ifood.gold.vw_taxi_trips`
* Dicionário de campos: `SELECT * FROM ifood.landing.dicionario_dados`

As queries de `analysis/` rodam direto no Databricks SQL (editor de queries) ou em
qualquer warehouse com acesso ao catálogo.
