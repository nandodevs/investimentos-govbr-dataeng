# Investimentos GovBR DataEng

## 📜 Descrição
Este projeto realiza o processamento e análise dos **Investimentos Públicos do Governo Brasileiro no ano de 2024**, utilizando um pipeline de engenharia de dados completo. Ele inclui etapas de ETL (Extração, Transformação e Carga), armazenamento em cloud (AWS S3), orquestração de tarefas com Apache Airflow e exposição de dados via API (FastAPI).

O objetivo é demonstrar habilidades práticas de engenharia de dados em um caso de uso real.

---

## 🚀 Tecnologias Utilizadas
- **Python**: Scripts de ETL.
- **Apache Airflow**: Orquestração do pipeline.
- **AWS S3**: Armazenamento dos dados brutos.
- **PostgreSQL**: Banco de dados relacional.
- **FastAPI**: Exposição de dados via API.
- **Docker**: Contêineres para facilitar o desenvolvimento e a execução.
- **pytest**: Testes automatizados para garantir qualidade.

---

## 🛠️ Funcionalidades
- **ETL Completo**:
  - Extração do dataset público de investimentos.
  - Transformação dos dados para correção e análise.
  - Carga dos dados em um banco relacional.
- **Armazenamento em S3**:
  - Salvamento dos dados brutos e transformados.
- **Orquestração com Airflow**:
  - Automação e monitoramento de todo o pipeline.
- **API com FastAPI**:
  - Consulta de dados processados via endpoints REST.

---

## 📂 Estrutura do Projeto
```plaintext
investimentos-govbr-dataeng/
├── dags/                    # Pipelines do Airflow
├── data/                    # Dados brutos e processados
├── scripts/                 # Scripts de ETL
├── api/                     # API com FastAPI
├── tests/                   # Testes automatizados
├── docker/                  # Configurações Docker
├── logs/                    # Logs do projeto
├── config/                  # Configurações do projeto
├── notebooks/               # Análises exploratórias
├── requirements.txt         # Dependências do projeto
└── README.md                # Documentação do projeto
```

## 📊 Dataset
- Fonte: Tesouro Transparente
- Descrição: Informações detalhadas sobre investimentos públicos do Governo Brasileiro no ano de 2024.
- Formato: CSV

---

## ⚙️ Como Executar o Projeto
- Pré-requisitos
- Python 3.8+
- Docker e Docker Compose
- Credenciais AWS configuradas

**Etapas:**
1. Clone este repositório:

    ```bash
    git clone https://github.com/seu-usuario/investimentos-govbr-dataeng.git
    ```

2. Navegue até o diretório do projeto:
    ```bash
    cd investimentos-govbr-dataeng
    ```

3. Crie um ambiente virtual e instale as dependências:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

4. Execute o Docker Compose para subir os serviços:
    ```bash
    docker-compose up
    ```

## 📈 Próximas Etapas
 - Criar o pipeline inicial no Airflow.
 - Configurar o bucket S3 para armazenamento.
 - Construir os scripts de ETL.
 - Implementar a API com FastAPI.

## 📝 Autor
### Sisnando Junior

[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/sisnando-junior/)
