import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import io
import re
import numpy as np
import xlsxwriter
import os

# --- CONFIG ---
PAGE_TITLE = "ANP Produção de Petróleo e Gás"
DATA_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/fase-de-desenvolvimento-e-producao"

# --- HELPER FUNCTIONS ---

def get_available_years():
    """
    Scrapes the ANP website to find available years and their CSV links,
    categorizing them by Environment (Terra/Mar).
    """
    years_data = {}
    
    try:
        response = requests.get(DATA_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            text = link.get_text().strip()
            href = link['href']
            href_lower = href.lower()
            text_lower = text.lower()
            
            # Find year in text (e.g. "2024", "Dados 2024", "2024-2025")
            # Look for 4 digits starting with 19 or 20
            year_match = re.search(r'(19|20)\d{2}', text)
            
            if year_match and '.csv' in href_lower:
                year_key = year_match.group(0)
                
                # Determine Environment (Check both HREF and TEXT)
                env = None
                
                # Keywords
                mar_keywords = ['mar', 'offshore', 'producao_mar', 'marítima']
                terra_keywords = ['terra', 'terrestre', 'onshore', 'producao_terra']
                
                is_mar = any(k in href_lower for k in mar_keywords) or any(k in text_lower for k in mar_keywords)
                is_terra = any(k in href_lower for k in terra_keywords) or any(k in text_lower for k in terra_keywords)
                
                if is_mar and not is_terra:
                    env = 'Mar'
                elif is_terra and not is_mar:
                    env = 'Terra'
                elif is_mar and is_terra:
                    # Ambiguous, trust href or assign to both? 
                    # Usually "Terra e Mar" doesn't happen in one CSV for these separate lists.
                    # Defaulting to Mar if ambiguous might be risky, but let's see. 
                    # If text says "Terra" and "Mar", maybe it's a combined file.
                    # For now, if "terra" appears, treat as Terra, unless "mar" is also strong.
                    # Let's prioritize explicit filenames.
                    if 'mar' in href_lower:
                        env = 'Mar'
                    else:
                        env = 'Terra'
                else:
                    # No keywords found. 
                    # If the link text is JUST the year, maybe looking at parents/headers is needed?
                    # But often filenames have hints (producao_mar_...).
                    # If we can't tell, we might skip or categorize as "Indefinido".
                    pass

                if env:
                    if year_key not in years_data:
                        years_data[year_key] = {}
                    
                    if env not in years_data[year_key]:
                        years_data[year_key][env] = []
                    
                    if href not in years_data[year_key][env]:
                        years_data[year_key][env].append(href)

        return dict(sorted(years_data.items(), key=lambda item: item[0], reverse=True))

    except Exception as e:
        st.error(f"Erro ao buscar dados do site: {e}")
        return {}

def sort_months(months):
    """Ordena meses garantindo ordem numérica se possível."""
    try:
        return sorted(months, key=lambda x: int(x) if str(x).isdigit() else x)
    except:
        return sorted(months)


def process_dataframe(df):
    """
    Cleans and processes the DataFrame.
    """
    
    # --- CORREÇÃO: SEPARAR MÊS/ANO SE NECESSÁRIO ---
    # Se tiver "Mês/Ano" (ex: 01/2025) mas não tiver "Mês" e "Ano" separados
    if 'Mês/Ano' in df.columns and ('Mês' not in df.columns or 'Ano' not in df.columns):
        try:
            # Tenta separar pela barra
            df[['Mês', 'Ano']] = df['Mês/Ano'].astype(str).str.split('/', expand=True)
            # Converte para numérico para facilitar ordenação
            df['Mês'] = pd.to_numeric(df['Mês'], errors='coerce')
            df['Ano'] = pd.to_numeric(df['Ano'], errors='coerce')
        except Exception:
            pass

    # Columns to convert
    cols_to_convert = [
        "Produção de Óleo (m³)", 
        "Produção de Gás Associado (Mm³)", 
        "Produção de Gás Não Associado (Mm³)", 
        "Produção de Água (m³)", 
        "Injeção de Gás (Mm³)", 
        "Injeção de Água para Recuperação Secundária (m³)", 
        "Injeção de Água para Descarte (m³)", 
        "Injeção de Gás Carbônico (Mm³)", 
        "Injeção de Nitrogênio (Mm³)", 
        "Injeção de Vapor de Água (t)"
    ]
    
    valid_cols = [c for c in cols_to_convert if c in df.columns]
    
    for col in valid_cols:
        if not pd.api.types.is_numeric_dtype(df[col]):
            try:
                df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            except Exception:
                pass
    
    # Remove unwanted columns
    cols_to_drop = [
        "Bacia", "Instalação", "Estado", 
        "Produção de Condensado (m³)", "Injeção de Polímeros (m³)", 
        "Injeção de Outros Fluidos (m³)"
    ]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')
    
    # --- CÁLCULOS DE ENGENHARIA ---
    
    # A. PREPARAÇÃO DA DATA
    if 'Ano' in df.columns and 'Mês' in df.columns:
        df['Data_Temp'] = pd.to_datetime(df['Ano'].astype(str) + '-' + df['Mês'].astype(str) + '-01', errors='coerce')
        df = df.sort_values(by=['Poço', 'Data_Temp'])
        
        # C. TEMPO
        df['tempo'] = df.groupby('Poço')['Data_Temp'].transform(lambda x: (x - x.min()).dt.days)
        
        # E. Np
        df['Np'] = df.groupby('Poço')['Produção de Óleo (m³)'].cumsum()
        
        # Remove Data_Temp para não poluir, se quiser
        df = df.drop(columns=['Data_Temp']) 
    else:
        df['tempo'] = 0
        df['Np'] = 0

    # B. RGO
    gas_total_m3 = (df.get("Produção de Gás Associado (Mm³)", 0) + df.get("Produção de Gás Não Associado (Mm³)", 0)) * 1000
    
    df['RGO'] = np.where(df['Produção de Óleo (m³)'] > 0, 
                         gas_total_m3 / df['Produção de Óleo (m³)'], 
                         0)

    # RAO
    df['RAO'] = np.where(df['Produção de Óleo (m³)'] > 0, 
                         df.get('Produção de Água (m³)', 0) / df['Produção de Óleo (m³)'], 
                         0)

    # D. lnq
    df['lnq'] = np.nan
    mask_oleo_positivo = df['Produção de Óleo (m³)'] > 0
    df.loc[mask_oleo_positivo, 'lnq'] = np.log(df.loc[mask_oleo_positivo, 'Produção de Óleo (m³)'])

    return df

@st.cache_data(show_spinner=False)
def load_csv(url):
    """
    Loads a single CSV from URL, cleans it, and returns a DataFrame.
    Does NOT cache here because we might concat multiple; better to cache the higher level if efficient,
    or rely on this being fast enough. For reliability with 'Ambos', we call this twice.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        csv_content = io.BytesIO(response.content)
        
        # Try reading with different encodings
        try:
             df = pd.read_csv(csv_content, sep=',', encoding='windows-1252', on_bad_lines='skip')
        except UnicodeDecodeError:
             csv_content.seek(0)
             df = pd.read_csv(csv_content, sep=',', encoding='utf-8', on_bad_lines='skip')
             
        # Normalize columns: remove brackets, trim whitespace
        df.columns = df.columns.str.replace(r'[\[\]]', '', regex=True).str.strip()
        
        return df
    except Exception as e:
        st.error(f"Erro ao baixar/ler URL {url}: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner=True)
def get_dataset(urls):
    """
    Fetches and processes data from a list of URLs.
    Concatenates them if multiple are provided.
    Runs process_dataframe on the final result.
    """
    dfs = []
    progress_text = "Baixando dados..."
    my_bar = st.progress(0, text=progress_text)
    
    total = len(urls)
    for i, url in enumerate(urls):
        df_part = load_csv(url)
        if not df_part.empty:
            dfs.append(df_part)
        my_bar.progress((i + 1) / total, text=f"Baixando parte {i+1} de {total}...")
            
    my_bar.empty()
    
    if not dfs:
        return pd.DataFrame()
        
    final_df = pd.concat(dfs, ignore_index=True)
    final_df = process_dataframe(final_df)
    return final_df

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        worksheet = writer.sheets['Sheet1']
        (max_row, max_col) = df.shape
        column_settings = [{'header': column} for column in df.columns]
        worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
        worksheet.set_column(0, max_col - 1, 15)
    return output.getvalue()

def manage_local_database(force_update=False):
    """
    Gerencia o banco de dados local.
    Se o arquivo existir, carrega. Se não (ou se forçar atualização), baixa tudo da ANP.
    """
    FILE_NAME = "dados_anp_completo.parquet"
    
    if os.path.exists(FILE_NAME) and not force_update:
        try:
            return pd.read_parquet(FILE_NAME)
        except Exception:
            st.warning("Arquivo local corrompido. Baixando novamente...")
    
    # Se chegou aqui, precisa baixar
    st.info("Iniciando download completo da base de dados da ANP. Isso pode demorar alguns minutos...")
    
    years_data = get_available_years()
    if not years_data:
        return pd.DataFrame()
    
    # Coletar todas as URLs separadas por ambiente
    urls_terra = []
    urls_mar = []
    
    for year, envs in years_data.items():
        if 'Terra' in envs:
            urls_terra.extend(envs['Terra'])
        if 'Mar' in envs:
            urls_mar.extend(envs['Mar'])
            
    # Baixar e processar
    df_terra = pd.DataFrame()
    df_mar = pd.DataFrame()
    
    if urls_terra:
        st.write(f"Baixando {len(urls_terra)} arquivos de Terra...")
        df_terra = get_dataset(urls_terra)
        if not df_terra.empty:
            df_terra['Ambiente'] = 'Terra'
            
    if urls_mar:
        st.write(f"Baixando {len(urls_mar)} arquivos de Mar...")
        df_mar = get_dataset(urls_mar)
        if not df_mar.empty:
            df_mar['Ambiente'] = 'Mar'
    
    # Juntar tudo
    full_df = pd.concat([df_terra, df_mar], ignore_index=True)
    
    # Salvar localmente para a próxima vez ser rápida
    if not full_df.empty:
        # Converter colunas object para string para compatibilidade com parquet
        full_df.columns = full_df.columns.astype(str)
        full_df.to_parquet(FILE_NAME, index=False)
        st.success("Base de dados atualizada e salva localmente!")
        
    return full_df

# --- MAIN APP ---

def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)

    # --- BARRA LATERAL ---
    st.sidebar.header("1. Configurações")
    
    # Botão para forçar atualização (caso saiam dados novos na ANP)
    if st.sidebar.button("🔄 Atualizar Base de Dados (Download)"):
        st.session_state['data'] = manage_local_database(force_update=True)
    
    # Carregamento Inicial (Automático ou via Cache)
    if 'data' not in st.session_state:
        with st.spinner("Carregando base de dados local..."):
            st.session_state['data'] = manage_local_database(force_update=False)

    df = st.session_state.get('data', pd.DataFrame())

    if not df.empty:
        # --- FILTROS DA BARRA LATERAL (Obrigatórios) ---
        
        # 1. Escolha Terra ou Mar
        # Verifica se a coluna Ambiente existe (criada na nossa nova função)
        if 'Ambiente' in df.columns:
            ambientes = sorted(df['Ambiente'].unique())
            selected_env = st.sidebar.radio("Ambiente", ambientes)
            # Filtra o DF globalmente pelo ambiente
            df = df[df['Ambiente'] == selected_env]
        
        st.sidebar.markdown("---")
        
        # 2. Escolha o Campo (Agora já temos todos os campos do ambiente carregados)
        if "Campo" in df.columns:
            campos_disponiveis = sorted(df["Campo"].dropna().astype(str).unique())
            sel_campos = st.sidebar.multiselect(
                "Selecione o Campo", 
                options=campos_disponiveis,
                placeholder="Escolha um ou mais campos"
            )
            
            if sel_campos:
                df = df[df["Campo"].isin(sel_campos)]
            else:
                # Opcional: Se não escolher campo, mostra aviso ou mostra tudo?
                # Geralmente é bom pedir para selecionar para não travar o navegador com muitos dados
                st.info("👈 Selecione um ou mais Campos na barra lateral para visualizar os dados.")
                st.stop() # Para a execução aqui até selecionar um campo

        # --- ÁREA PRINCIPAL (Filtros de Data e Poço) ---
        
        st.markdown("### 🔍 Filtros de Período e Poço")
        col1, col2, col3 = st.columns(3)
        
        # Filtro de Ano
        with col1:
            if "Ano" in df.columns:
                anos_disp = sorted(df["Ano"].unique(), reverse=True)
                sel_anos = st.multiselect("Ano", anos_disp)
                if sel_anos:
                    df = df[df["Ano"].isin(sel_anos)]
        
        # Filtro de Mês
        with col2:
            month_col = next((c for c in ['Mês', 'Mes', 'Month'] if c in df.columns), None)
            if month_col:
                meses = df[month_col].dropna().unique()
                try:
                    meses = sorted(meses, key=lambda x: int(x))
                except:
                    meses = sorted(meses)
                sel_meses = st.multiselect("Mês", meses)
                if sel_meses:
                    df = df[df[month_col].isin(sel_meses)]
                    
        # Filtro de Poço
        with col3:
            if "Poço" in df.columns:
                pocos = sorted(df["Poço"].dropna().astype(str).unique())
                sel_pocos = st.multiselect("Poço", pocos)
                if sel_pocos:
                    df = df[df["Poço"].isin(sel_pocos)]

        st.divider()
        
        # --- RESULTADOS ---
        st.markdown(f"### 📊 Dados Filtrados")
        st.write(f"**Registros:** {len(df):,}")
        
        st.dataframe(df, use_container_width=True)
        
        if not df.empty:
            excel_bytes = to_excel(df)
            st.download_button(
                label="📥 Baixar Excel",
                data=excel_bytes,
                file_name="Producao_ANP_Filtrada.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    else:
        st.warning("A base de dados está vazia. Tente clicar em 'Atualizar Base de Dados'.")

if __name__ == "__main__":
    main()

