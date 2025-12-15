import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import io
import re
import numpy as np

# Configuração da Página
PAGE_TITLE = "ANP Produção de Petróleo e Gás"
DATA_URL = "https://dados.gov.br/dados/conjuntos-dados/producao-de-petroleo-e-gas-natural-por-poco"

# --- HELPER FUNCTIONS ---

def get_available_years():
    """
    Varre o site da ANP e agrupa todos os links de CSV por ano.
    Se houver arquivos separados (Terra/Mar) para o mesmo ano, guarda ambos.
    Retorna: dict { '2025': ['url1', 'url2'], '2024': ['url1'] ... }
    """
    try:
        response = requests.get(DATA_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        years_links = {}
        
        for link in soup.find_all('a', href=True):
            text = link.get_text().strip()
            href = link['href']
            
            # Pula links que não sejam CSV ou ZIP (alguns meses vêm zipados)
            if not ('.csv' in href.lower() or '.zip' in href.lower()):
                continue

            # Tenta encontrar um ano (4 dígitos entre 2000 e 2099) no texto do link
            # Ex: "Produção por Poço - 2024" -> encontra 2024
            match = re.search(r'(20\d{2})', text)
            
            if match:
                year = match.group(1)
                
                # Filtro de segurança para pegar apenas links relevantes de produção
                keywords = ['producao', 'produção', 'poço', 'poco', 'mar', 'terra']
                if any(k in href.lower() or k in text.lower() for k in keywords):
                    if year not in years_links:
                        years_links[year] = []
                    
                    # Evita duplicatas
                    if href not in years_links[year]:
                        years_links[year].append(href)

        # Ordena os anos do mais recente para o mais antigo
        return dict(sorted(years_links.items(), key=lambda item: item[0], reverse=True))

    except Exception as e:
        st.error(f"Erro ao buscar dados do site: {e}")
        return {}

def process_dataframe(df):
    """
    Limpa, converte tipos e adiciona cálculos de engenharia.
    """
    # 1. Padronização de Nomes de Colunas (Remove espaços extras e colchetes)
    df.columns = df.columns.str.replace(r'[\[\]]', '', regex=True).str.strip()
    
    # 2. Conversão Numérica (PT-BR -> Float)
    cols_to_convert = [
        "Produção de Óleo (m³)", "Produção de Gás Associado (Mm³)", 
        "Produção de Gás Não Associado (Mm³)", "Produção de Água (m³)", 
        "Injeção de Gás (Mm³)", "Injeção de Água para Recuperação Secundária (m³)", 
        "Injeção de Água para Descarte (m³)", "Injeção de Gás Carbônico (Mm³)", 
        "Injeção de Nitrogênio (Mm³)", "Injeção de Vapor de Água (t)"
    ]
    
    valid_cols = [c for c in cols_to_convert if c in df.columns]
    
    for col in valid_cols:
        if not pd.api.types.is_numeric_dtype(df[col]):
            try:
                df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            except Exception:
                pass
    
    # 3. Remoção de Colunas Irrelevantes
    cols_to_drop = [
        "Bacia", "Instalação", "Estado", "Ambiente", 
        "Produção de Condensado (m³)", "Injeção de Polímeros (m³)", 
        "Injeção de Outros Fluidos (m³)"
    ]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')

    # 4. Cálculos de Engenharia
    if 'Ano' in df.columns and 'Mês' in df.columns:
        # Cria data para ordenação
        df['Data_Temp'] = pd.to_datetime(df['Ano'].astype(str) + '-' + df['Mês'].astype(str) + '-01', errors='coerce')
        df = df.sort_values(by=['Poço', 'Data_Temp'])
        
        # Tempo (dias) e Np (Acumulado)
        df['tempo'] = df.groupby('Poço')['Data_Temp'].transform(lambda x: (x - x.min()).dt.days)
        df['Np'] = df.groupby('Poço')['Produção de Óleo (m³)'].cumsum()
        df = df.drop(columns=['Data_Temp'])
    else:
        df['tempo'] = 0
        df['Np'] = 0

    # RGO e RAO
    gas_total_m3 = (df.get("Produção de Gás Associado (Mm³)", 0) + df.get("Produção de Gás Não Associado (Mm³)", 0)) * 1000
    
    with np.errstate(divide='ignore', invalid='ignore'):
        df['RGO'] = gas_total_m3 / df['Produção de Óleo (m³)']
        df['RAO'] = df['Produção de Água (m³)'] / df['Produção de Óleo (m³)']
    
    df['RGO'] = df['RGO'].replace([np.inf, -np.inf], 0).fillna(0)
    df['RAO'] = df['RAO'].replace([np.inf, -np.inf], 0).fillna(0)

    # lnq
    df['lnq'] = np.nan
    mask_oleo = df['Produção de Óleo (m³)'] > 0
    df.loc[mask_oleo, 'lnq'] = np.log(df.loc[mask_oleo, 'Produção de Óleo (m³)'])
    
    return df

@st.cache_data(show_spinner=True)
def load_data(urls):
    """
    Baixa um ou múltiplos CSVs (ex: Terra + Mar) e combina em um único DataFrame.
    """
    all_dfs = []
    
    # Garante que urls seja uma lista
    if isinstance(urls, str):
        urls = [urls]
        
    for url in urls:
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            # Se for ZIP, precisaria de tratamento extra, mas aqui focamos no CSV padrão
            # O pandas lê ZIP automaticamente se for um único arquivo dentro, 
            # mas se a URL terminar em .csv, lemos direto.
            
            content = io.BytesIO(response.content)
            
            try:
                df_temp = pd.read_csv(content, sep=',', encoding='windows-1252', on_bad_lines='skip')
            except UnicodeDecodeError:
                content.seek(0)
                df_temp = pd.read_csv(content, sep=',', encoding='utf-8', on_bad_lines='skip')
            except Exception:
                # Tenta ponto e vírgula se falhar
                content.seek(0)
                df_temp = pd.read_csv(content, sep=';', encoding='latin1', on_bad_lines='skip')

            all_dfs.append(df_temp)
            
        except Exception as e:
            st.warning(f"Falha ao baixar um dos arquivos ({url}): {e}")

    if not all_dfs:
        return pd.DataFrame()
        
    # Combina Terra e Mar (se houver múltiplos arquivos)
    final_df = pd.concat(all_dfs, ignore_index=True)
    
    # Processa tudo junto
    return process_dataframe(final_df)

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Dados ANP')
        workbook = writer.book
        worksheet = writer.sheets['Dados ANP']
        (max_row, max_col) = df.shape
        column_settings = [{'header': column} for column in df.columns]
        worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
        worksheet.set_column(0, max_col - 1, 15)
    return output.getvalue()

# --- MAIN APP ---

def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)
    
    st.sidebar.header("Configurações")
    
    # 1. Busca Anos
    with st.spinner("Varrendo site da ANP em busca de arquivos..."):
        available_years = get_available_years()
    
    if available_years:
        selected_year = st.sidebar.selectbox("Selecione o Ano", options=list(available_years.keys()))
        urls = available_years[selected_year]
        
        st.sidebar.success(f"Arquivos encontrados para {selected_year}: {len(urls)}")
        # Mostra quais arquivos serão baixados (debug visual para o usuário)
        with st.sidebar.expander("Ver links fonte"):
            for u in urls:
                st.write(u)
    else:
        st.warning("Não foi possível encontrar anos automaticamente.")
        url_manual = st.sidebar.text_input("Cole a URL do CSV manualmente")
        urls = [url_manual] if url_manual else []
        selected_year = "Manual"

    # 2. Botão de Download
    if st.sidebar.button("Baixar/Atualizar Dados"):
        if urls:
            st.session_state['data'] = load_data(urls)
            st.session_state['year'] = selected_year
        else:
            st.error("Nenhuma URL válida para baixar.")

    # 3. Visualização
    if 'data' in st.session_state and not st.session_state['data'].empty:
        df = st.session_state['data']
        
        st.markdown(f"### 📊 Dados Consolidados: {st.session_state.get('year', 'N/A')}")
        
        # Filtros
        col1, col2 = st.columns(2)
        filtered_df = df.copy()
        
        if "Campo" in filtered_df.columns:
            campos = sorted(filtered_df["Campo"].dropna().astype(str).unique().tolist())
            selected_campos = col1.multiselect("Filtrar por Campo", options=campos)
            if selected_campos:
                filtered_df = filtered_df[filtered_df["Campo"].isin(selected_campos)]
        
        if "Poço" in filtered_df.columns:
            # Filtra poços baseado no campo selecionado (se houver)
            pocos_disponiveis = sorted(filtered_df["Poço"].dropna().astype(str).unique().tolist())
            selected_pocos = col2.multiselect("Filtrar por Poço", options=pocos_disponiveis)
            if selected_pocos:
                filtered_df = filtered_df[filtered_df["Poço"].isin(selected_pocos)]
        
        st.info(f"Exibindo {len(filtered_df)} registros de {len(df)} totais.")
        st.dataframe(filtered_df, use_container_width=True)
        
        if not filtered_df.empty:
            st.download_button(
                label="📥 Baixar Excel (.xlsx)",
                data=to_excel(filtered_df),
                file_name=f"producao_anp_{st.session_state.get('year', 'dados')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
    elif 'data' in st.session_state:
        st.warning("Arquivo baixado, mas está vazio.")

if __name__ == "__main__":
    main()
```[[1](https://www.google.com/url?sa=E&q=https%3A%2F%2Fvertexaisearch.cloud.google.com%2Fgrounding-api-redirect%2FAUZIYQEizbIMVWRbgc1Nj2a8k5WNiWA7-tZX2AwPBZVN3EQjPlwbiAAr3CYVkoJBgUdi1GIpi4HDujB9hC5xHMLrgdTuKtvTMzTv8r95rfkYSmQSne8pQ9TNyaCjKx-C7PEx5DvfSYmct2DShQebnDi2dfMfbSY0q9XiuPuMfNvHJ-pCeee7ZafnMBliG3oLjQ%3D%3D)]




