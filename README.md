# 🛢️ ANP Data Explorer
Uma aplicação web simples e poderosa desenvolvida em Python e Streamlit para automatizar a coleta, filtragem e exportação de dados públicos de produção de petróleo e gás da **Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP)**.
## 📋 Funcionalidades
-   **Coleta Automática**: Varre o site da ANP para identificar os anos disponíveis (incluindo dados históricos desde 1941).
-   **Download Inteligente**: Baixa os arquivos CSV oficiais (Produção Marítima) diretamente da fonte.
-   **Processamento de Dados**:
    -   Converte formatação numérica brasileira (`1.234,56` -> `1234.56`).
    -   Limpa colunas desnecessárias para focar no que importa.
-   **Filtros Dinâmicos**: Filtre os dados por **Campo** e **Poço**.
-   **Exportação Excel**: Gere relatórios `.xlsx` limpos e formatados prontos para análise.
## 🚀 Tecnologias Utilizadas
-   **Frontend**: [Streamlit](https://streamlit.io/)
-   **Manipulação de Dados**: [Pandas](https://pandas.pydata.org/)
-   **Web Scraping**: [Requests](https://pypi.org/project/requests/) & [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/)
-   **Exportação**: [XlsxWriter](https://xlsxwriter.readthedocs.io/)
## 📦 Como rodar localmente
1.  **Clone o repositório** (ou baixe os arquivos):
    ```bash
    git clone https://github.com/seu-usuario/anp-data-explorer.git
    cd anp-data-explorer
    ```
2.  **Instale as dependências**:
    Recomenda-se usar um ambiente virtual (`venv`).
    ```bash
    pip install -r requirements.txt
    ```
3.  **Execute a aplicação**:
    ```bash
    streamlit run app.py
    ```
4.  **Acesse no navegador**:
    O app abrirá automaticamente em `http://localhost:8501`.
## 🌐 Deploy na Web
Esta aplicação é compatível com o **Streamlit Community Cloud**.
Basta subir este código para um repositório GitHub e conectar sua conta do Streamlit Cloud.
## 📄 Fonte dos Dados
Todos os dados são públicos e obtidos diretamente do portal de Dados Abertos da ANP:
[Fase de Desenvolvimento e Produção](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/fase-de-desenvolvimento-e-producao)
---
Desenvolvido com 🐍 Python.
