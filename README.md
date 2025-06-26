#  Pipeline de Processamento de Dados com Spark, Kafka e Docker

Este projeto implementa um pipeline distribuído para ingestão, tratamento, análise e visualização de dados de saúde pública, utilizando Apache Kafka, Apache Spark, Redis, FastAPI e Streamlit, tudo orquestrado com Docker Compose.

##  Arquitetura

O sistema é composto por diversos microserviços:

- **Kafka + Zookeeper**: Mensageria distribuída para comunicação entre os módulos.
- **Redis**: Armazenamento auxiliar (cache e comunicação leve).
- **Mock Generator**: Gera dados sintéticos (OMS, hospitais, secretaria).
- **Tratadores Spark (1 a 9)**: Realizam limpeza, filtragem, agregação, fusão, correlação, regressão, desvio padrão e média móvel.
- **FastAPI**: Fornece uma API intermediária para expor dados processados.
- **Streamlit Dashboard**: Visualiza os resultados em tempo real.

##  Requisitos

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

##  Como Executar

1. Clone o repositório:

```bash
git clone https://github.com/PAULA-123/ScalableComputingA2
cd ScalableComputingA2
```

ou baixe o projeto completo zip


2. Construa e suba todos os serviços:

```bash
docker-compose up --build

```
Os serviços serão iniciados em modo interativo. Use -d para rodar em segundo plano.

3. Acesse o dashboard na URL:

```bash
http://localhost:8501

```

4. Acesse a API FastAPI (com documentação Swagger):

```bash
http://localhost:8000/docs

```

5. Para encerrar os serviços:

```bash
docker-compose down

```

6. (Opcional) Limpeza de volumes e imagens:

```bash
docker system prune -a
docker volume prune

```

##  Estrutura dos Diretórios

```bash
.
├── api/                      # API intermediária (FastAPI)
├── dashboard/                # Dashboard interativo (Streamlit)
├── mock_data/                # Gerador de dados simulados (mock)
├── tratador1_limpeza/
├── tratador2_filtragem/
├── tratador_4_agrupar_colunas/
├── tratador_6_correlacao/
├── tratador_7_desviopadrao/
├── tratador8_regressao/
├── tratador_9_mediamovel/
├── tratador_3_merge/        # Merge hospitalar + secretaria
├── tratador_5_alerta/       # Alerta por média populacional
├── docker-compose.yml
└── README.md

```



## Funcionalidades

- Ingestão de dados sintéticos de múltiplas fontes (OMS, hospitais, secretaria).

- Tratamento paralelo com Spark estruturado.

- Análises como:

    - Correlação entre vacinação e escolaridade

    - Média móvel de infectados

    - Regressão linear multivariada

    - Alertas populacionais

    - Desvios padrão regionais

Visualização dos resultados via dashboard


#### Alunos: Ana Júlia Amaro Pereira Rocha, Henrique Borges Carvalho, Maria Eduarda Mesquita Magalhães, Mariana Fernandes Rocha e Paula Eduarda de Lima.