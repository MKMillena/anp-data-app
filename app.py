import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import io
import re
import numpy as np

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
                    years_data[year_key][env] = href

        return dict(sorted(years_data.items(), key=lambda item: item[0], reverse=True))

    except Exception as e:
        st.error(f"Erro ao buscar dados do site: {e}")
        return {}

def process_dataframe(df):
    """
    Cleans and processes the DataFrame.
    """
    
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
    
    # Remove unwanted columns - KEEP 'Ambiente' now to distinguish Land/Sea
    cols_to_drop = [
        "Bacia", 
        "Instalação", 
        "Estado", 
        # "Ambiente",  <-- REMOVED from drop list to keep it
        "Produção de Condensado (m³)", 
        "Injeção de Polímeros (m³)", 
        "Injeção de Outros Fluidos (m³)"
    ]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')
    
    # --- CÁLCULOS DE ENGENHARIA ---
    
    # A. PREPARAÇÃO DA DATA
    if 'Ano' in df.columns and 'Mês' in df.columns:
        # Mapeamento de meses se estiverem em texto, mas geralmente no CSV ANP 'Mês' é numérico ou texto simples.
        # Converter para garantir.
        # Se for string numeral "01", "1", ok. Se for "Janeiro", precisaria de map. 
        # Assumindo numerico conforme padrão observado, mas forçando string para criar data.
        
        df['Data_Temp'] = pd.to_datetime(df['Ano'].astype(str) + '-' + df['Mês'].astype(str) + '-01', errors='coerce')
        
        # Ordena por Poço e Data
        df = df.sort_values(by=['Poço', 'Data_Temp'])
        
        # C. TEMPO (Dias desde o primeiro registro do poço)
        df['tempo'] = df.groupby('Poço')['Data_Temp'].transform(lambda x: (x - x.min()).dt.days)
        
        # E. Np (Produção Acumulada de Óleo por Poço)
        # Converter m3 para bbl? O pedido original não especificou, manteve a unidade da coluna base (m3).
        df['Np'] = df.groupby('Poço')['Produção de Óleo (m³)'].cumsum()
        
        # Manter Data_Temp para filtros de data se necessário, ou remover.
        # Como pedido "Filtro de Mês", é bom ter Mês limpo, mas a coluna Mês original é usada.
        # df = df.drop(columns=['Data_Temp']) 
    else:
        df['tempo'] = 0
        df['Np'] = 0

    # B. RGO (Razão Gás-Óleo)
    # Gás Total (Mm³ * 1000 = m³) / Óleo (m³)
    gas_total_m3 = (df["Produção de Gás Associado (Mm³)"] + df["Produção de Gás Não Associado (Mm³)"]) * 1000
    
    # Evitar divisão por zero e NaNs
    df['RGO'] = np.where(df['Produção de Óleo (m³)'] > 0, 
                         gas_total_m3 / df['Produção de Óleo (m³)'], 
                         0)

    # RAO (Razão Água-Óleo)
    df['RAO'] = np.where(df['Produção de Óleo (m³)'] > 0, 
                         df['Produção de Água (m³)'] / df['Produção de Óleo (m³)'], 
                         0)

    # D. lnq (Logaritmo Natural da Vazão de Óleo)
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

# --- MAIN APP ---

def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)
    
    st.sidebar.header("Configurações")
    
    # 1. Scraping Years
    with st.spinner("Conectando ao site da ANP..."):
        annotated_years = get_available_years()
    
    if annotated_years:
        # Year Selection
        selected_year = st.sidebar.selectbox("1. Selecione o Ano", options=list(annotated_years.keys()))
        
        # Environment Selection (based on what's available for that year)
        available_envs = annotated_years[selected_year] # e.g. {'Terra': url, 'Mar': url}
        
        env_options = list(available_envs.keys())
        if 'Terra' in env_options and 'Mar' in env_options:
            display_options = ['Terra', 'Mar', 'Ambos']
        else:
            display_options = env_options
            
        selected_env = st.sidebar.radio("2. Selecione o Ambiente", display_options)
        
        # Determine URLs to download
        urls_to_download = []
        if selected_env == 'Ambos':
            urls_to_download = [available_envs['Terra'], available_envs['Mar']]
        elif selected_env in available_envs:
            urls_to_download = [available_envs[selected_env]]
            
        st.sidebar.info(f"Arquivos identificados: {len(urls_to_download)}")

        # Download Button
        if st.sidebar.button("Baixar Dados"):
            if urls_to_download:
                st.session_state['data'] = get_dataset(urls_to_download)
                st.session_state['year'] = selected_year
                st.session_state['env'] = selected_env
            else:
                st.error("Erro ao identificar URLs de download.")

    else:
        st.error("Não foi possível carregar a lista de anos do site da ANP.")
        st.warning("Verifique sua conexão ou se o site da ANP mudou de estrutura.")

    # 3. Data View
    if 'data' in st.session_state and not st.session_state['data'].empty:
        df = st.session_state['data']
        
        st.divider()
        st.markdown(f"### 📊 Análise: {st.session_state['year']} - {st.session_state['env']}")
        st.write(f"**Total de Registros:** {len(df):,}")
        
        # --- NEW MONTH FILTER ---
        if 'Mês' in df.columns:
            # Sort months naturally if they are numbers or text numbers
            try:
                unique_months = sorted(df['Mês'].unique(), key=lambda x: int(x) if str(x).isdigit() else x)
            except:
                unique_months = sorted(df['Mês'].astype(str).unique())
                
            selected_months = st.multiselect("📅 Filtrar por Mês(es)", options=unique_months, placeholder="Selecione um ou mais meses (deixe vazio para todos)")
            
            if selected_months:
                df = df[df['Mês'].isin(selected_months)]
                st.caption(f"Filtrado para meses: {', '.join(map(str, selected_months))}")

        # --- EXISTING FILTERS (Campo / Poço) ---
        col1, col2 = st.columns(2)
        
        filtered_df = df.copy()
        
        # Filter by Campo
        if "Campo" in filtered_df.columns:
            campos = sorted(filtered_df["Campo"].dropna().astype(str).unique())
            sel_campos = col1.multiselect("Filtrar por Campo", campos)
            if sel_campos:
                filtered_df = filtered_df[filtered_df["Campo"].isin(sel_campos)]
        
        # Filter by Poço
        if "Poço" in filtered_df.columns:
            # Update wells based on current filtered_df (which might be filtered by Campo)
            pocos = sorted(filtered_df["Poço"].dropna().astype(str).unique())
            sel_pocos = col2.multiselect("Filtrar por Poço", pocos)
            if sel_pocos:
                filtered_df = filtered_df[filtered_df["Poço"].isin(sel_pocos)]
        
        st.dataframe(filtered_df, use_container_width=True)
        
        # --- EXPORT ---
        st.markdown("### Exportação")
        if not filtered_df.empty:
            excel_bytes = to_excel(filtered_df)
            st.download_button(
                label="📥 Baixar Planilha Excel (.xlsx)",
                data=excel_bytes,
                file_name=f"Producao_ANP_{st.session_state['year']}_{st.session_state['env']}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("A tabela está vazia com os filtros atuais.")

if __name__ == "__main__":
    main()
