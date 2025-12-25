import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import io
import re
import numpy as np
import xlsxwriter
import json
import os
import glob

# --- CONFIG ---
PAGE_TITLE = "ANP Produção de Petróleo e Gás"
DATA_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/fase-de-desenvolvimento-e-producao"
DOWNLOAD_DIR = "anp_data"
METADATA_DIR = "anp_metadata"

# --- HELPER FUNCTIONS ---

def ensure_dirs():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
    if not os.path.exists(METADATA_DIR):
        os.makedirs(METADATA_DIR)

def get_metadata_path(env):
    return os.path.join(METADATA_DIR, f"campos_{env}.json")

def save_metadata(env, campos):
    ensure_dirs()
    path = get_metadata_path(env)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(list(campos), f, ensure_ascii=False, indent=4)

def load_metadata(env):
    path = get_metadata_path(env)
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return sorted(json.load(f))
    return []

def get_available_files(target_env):
    """
    Scrapes the ANP website to find all available CSV links for the target environment.
    Returns a list of tuples: (year, filename, url)
    """
    files_found = []
    
    try:
        response = requests.get(DATA_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            text = link.get_text().strip()
            href = link['href']
            href_lower = href.lower()
            text_lower = text.lower()
            
            # Find year in text
            year_match = re.search(r'(19|20)\d{2}', text)
            
            if year_match and '.csv' in href_lower:
                year = year_match.group(0)
                
                # Determine Environment
                env = None
                mar_keywords = ['mar', 'offshore', 'producao_mar', 'marítima']
                terra_keywords = ['terra', 'terrestre', 'onshore', 'producao_terra']
                
                is_mar = any(k in href_lower for k in mar_keywords) or any(k in text_lower for k in mar_keywords)
                is_terra = any(k in href_lower for k in terra_keywords) or any(k in text_lower for k in terra_keywords)
                
                if is_mar and not is_terra:
                    env = 'Mar'
                elif is_terra and not is_mar:
                    env = 'Terra'
                elif is_mar and is_terra:
                    if 'mar' in href_lower:
                        env = 'Mar'
                    else:
                        env = 'Terra'
                
                if env == target_env:
                    # Extract filename from URL
                    filename = href.split('/')[-1]
                    files_found.append((year, filename, href))

        # Sort by year descending to ensure we get latest first
        files_found.sort(key=lambda x: x[0], reverse=True)
        return files_found

    except Exception as e:
        st.error(f"Erro ao buscar dados do site: {e}")
        return []

def download_file(url, local_path, progress_bar=None, progress_text=""):
    """
    Helper to download a single file.
    """
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        with open(local_path, 'wb') as f:
            f.write(resp.content)
        return True
    except Exception as e:
        st.error(f"Falha ao baixar {url}: {e}")
        return False

def update_metadata_cache(target_env, full_scan=False):
    """
    Downloads files to build the 'Campo' list cache.
    By default (full_scan=False), ONLY downloads the latest 3 files to be fast but robust.
    """
    ensure_dirs()
    
    st.info(f"Buscando lista de arquivos para: {target_env}...")
    files_to_process = get_available_files(target_env)
    
    if not files_to_process:
        st.warning("Nenhum arquivo encontrado.")
        return []

    # If Quick Scan, take only the most recent 3 files (list is sorted desc)
    # This increases chance of finding a valid file if one is broken/empty
    if not full_scan:
        files_to_process = files_to_process[:3] 
        st.caption("ℹ️ Modo Rápido: Verificando os 3 arquivos mais recentes para listar campos.")

    unique_campos = set()
    total = len(files_to_process)
    progress_bar = st.progress(0, text="Indexando campos...")
    
    for i, (year, filename, url) in enumerate(files_to_process):
        local_filename = f"{year}_{target_env}_{filename}"
        local_path = os.path.join(DOWNLOAD_DIR, local_filename)
        
        # Download if missing
        if not os.path.exists(local_path):
             progress_bar.progress(i / total, text=f"Baixando índice: {filename} ({year})...")
             if not download_file(url, local_path):
                 continue

        # Extract fields
        progress_bar.progress((i + 0.5) / total, text=f"Lendo campos: {filename}...")
        
        found_in_file = False
        for encoding in ['windows-1252', 'utf-8', 'latin1']:
            if found_in_file: break
            
            try:
                # Manual finding of header row
                header_row_idx = None
                sep = ','
                
                with open(local_path, 'r', encoding=encoding, errors='replace') as f:
                    # Scan first 100 lines for header
                    lines = [f.readline() for _ in range(100)]
                    
                for idx, line in enumerate(lines):
                    # Look for "Campo" or "campo" delimited
                    if 'Campo' in line:
                        header_row_idx = idx
                        if line.count(';') > line.count(','):
                            sep = ';'
                        break
                
                if header_row_idx is not None:
                    # Read using discovered metadata
                    df_iter = pd.read_csv(
                        local_path,
                        encoding=encoding,
                        sep=sep,
                        header=0, # Relative to skiprows
                        skiprows=header_row_idx,
                        nrows=None, # Read all rows for the unique list
                        on_bad_lines='skip',
                        usecols=lambda x: x and 'Campo' in x # Permissive column match
                    )
                    
                    if not df_iter.empty:
                        # Find the exact column that contains "Campo"
                        target_col = next((c for c in df_iter.columns if 'Campo' in str(c)), None)
                        if target_col:
                            unique_campos.update(df_iter[target_col].dropna().astype(str).unique())
                            found_in_file = True
            except Exception:
                continue

    progress_bar.empty()
    
    if not unique_campos:
        st.error("⚠️ Nenhum campo encontrado. Pode haver um erro nos arquivos da ANP ou na conexão.")
    else:
        st.success(f"Índice atualizado! {len(unique_campos)} campos encontrados.")
        
    save_metadata(target_env, unique_campos)
    return sorted(list(unique_campos))

def load_data_for_fields(target_env, selected_campos=None):
    """
    Ensures ALL files are present (downloads if missing) and loads data.
    Filters by selected_campos during load.
    """
    ensure_dirs()
    
    # 1. Identify all needed files
    files_to_process = get_available_files(target_env)
    total = len(files_to_process)
    
    if total == 0:
        return pd.DataFrame()

    dfs = []
    progress_bar = st.progress(0, text="Preparando download dos dados...")
    
    for i, (year, filename, url) in enumerate(files_to_process):
        local_filename = f"{year}_{target_env}_{filename}"
        local_path = os.path.join(DOWNLOAD_DIR, local_filename)
        
        # 2. DOWNLOAD (Deferred)
        if not os.path.exists(local_path):
            progress_bar.progress(i / total, text=f"Baixando histórico: {year}...")
            success = download_file(url, local_path)
            if not success:
                continue
        else:
             if i % 5 == 0:
                progress_bar.progress(i / total, text=f"Lendo arquivo local: {year}...")

        # 3. LOAD & FILTER
        try:
            try:
                 df_temp = pd.read_csv(local_path, sep=',', encoding='windows-1252', on_bad_lines='skip')
            except UnicodeDecodeError:
                 df_temp = pd.read_csv(local_path, sep=',', encoding='utf-8', on_bad_lines='skip')
            
            df_temp.columns = df_temp.columns.str.replace(r'[\[\]]', '', regex=True).str.strip()
            
            # FILTER ASAP
            if selected_campos and 'Campo' in df_temp.columns:
                df_temp = df_temp[df_temp['Campo'].isin(selected_campos)]
            
            if not df_temp.empty:
                dfs.append(df_temp)
        except Exception:
            pass
            
    progress_bar.empty()
    
    if not dfs:
        return pd.DataFrame()
    
    full_df = pd.concat(dfs, ignore_index=True)
    return process_dataframe(full_df)

def process_dataframe(df):
    """
    Cleans and processes the DataFrame.
    """
    
    # --- CORREÇÃO: SEPARAR MÊS/ANO SE NECESSÁRIO ---
    if 'Mês/Ano' in df.columns and ('Mês' not in df.columns or 'Ano' not in df.columns):
        try:
            df[['Mês', 'Ano']] = df['Mês/Ano'].astype(str).str.split('/', expand=True)
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
    if 'Ano' in df.columns and 'Mês' in df.columns:
        df['Data_Temp'] = pd.to_datetime(df['Ano'].astype(str) + '-' + df['Mês'].astype(str) + '-01', errors='coerce')
        df = df.sort_values(by=['Poço', 'Data_Temp'])
        df['tempo'] = df.groupby('Poço')['Data_Temp'].transform(lambda x: (x - x.min()).dt.days)
        df['Np'] = df.groupby('Poço')['Produção de Óleo (m³)'].cumsum()
        df = df.drop(columns=['Data_Temp']) 
    else:
        df['tempo'] = 0
        df['Np'] = 0

    # RGO / RAO
    gas_total_m3 = (df.get("Produção de Gás Associado (Mm³)", 0) + df.get("Produção de Gás Não Associado (Mm³)", 0)) * 1000
    df['RGO'] = np.where(df['Produção de Óleo (m³)'] > 0, gas_total_m3 / df['Produção de Óleo (m³)'], 0)
    df['RAO'] = np.where(df['Produção de Óleo (m³)'] > 0, df.get('Produção de Água (m³)', 0) / df['Produção de Óleo (m³)'], 0)

    # lnq
    df['lnq'] = np.nan
    mask_oleo_positivo = df['Produção de Óleo (m³)'] > 0
    df.loc[mask_oleo_positivo, 'lnq'] = np.log(df.loc[mask_oleo_positivo, 'Produção de Óleo (m³)'])

    return df

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
    
    st.sidebar.header("1. Configurações")
    
    # 1. Environment Selection (ALWAYS VISIBLE)
    selected_env = st.sidebar.radio("Ambiente", ["Terra", "Mar"])
    
    # 2. Check Cache for Fields
    cached_campos = load_metadata(selected_env)
    
    if not cached_campos:
        st.sidebar.warning("Lista de campos não encontrada.")
        if st.sidebar.button("📂 Obter Lista de Campos (Rápido)"):
            update_metadata_cache(selected_env, full_scan=False)
            st.rerun()
        
        # Option for full scan if needed
        with st.sidebar.expander("Opções Avançadas"):
             if st.button("🔄 Sincronizar Tudo (Lento)"):
                 update_metadata_cache(selected_env, full_scan=True)
                 st.rerun()
    else:
        st.sidebar.header("2. Seleção")
        
        # 3. FIELD SELECTION (From Cache)
        sel_campos = st.sidebar.multiselect(
            "Selecione o(s) Campo(s)", 
            options=cached_campos,
            placeholder="Escolha campos..."
        )
        
        # Wrapper for visual grouping
        load_cols = st.sidebar.columns([1])
        if load_cols[0].button("🚀 Carregar Dados Selecionados"):
             with st.spinner("Carregando..."):
                df = load_data_for_fields(selected_env, sel_campos if sel_campos else None)
                st.session_state['data'] = df
                st.session_state['env'] = selected_env
                st.session_state['campos_selecionados'] = sel_campos

    # 3. FIELD SELECTION & FILTERS (Only if data is loaded)
    if 'data' in st.session_state and not st.session_state['data'].empty:
        df = st.session_state['data']
        
        # Check consistency
        if st.session_state.get('env') != selected_env:
            st.warning("Ambiente alterado. Por favor, carregue os dados novamente.")
        else:
            st.sidebar.markdown("---")
            st.sidebar.header("3. Filtros Adicionais")
            
            # --- OTHER FILTERS (Year, Month, Well) ---
            
            # Year Filter
            if "Ano" in df.columns:
                unique_years = sorted(df["Ano"].dropna().unique(), reverse=True)
                sel_years = st.sidebar.multiselect("Filtrar Ano", unique_years)
                if sel_years:
                     df = df[df["Ano"].isin(sel_years)]

            # Month Filter
            month_col = None
            for col in ['Mês', 'Mes', 'Month']:
                if col in df.columns:
                    month_col = col
                    break
            
            if month_col:
                month_map = {
                    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
                    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
                }
                all_months = list(range(1, 13))
                def format_month(m):
                    return f"{month_map.get(m, m)} ({m})"

                sidebar_months = st.sidebar.multiselect("Filtrar Mês", all_months, format_func=format_month)
                if sidebar_months:
                    try:
                        is_numeric = pd.to_numeric(df[month_col], errors='coerce')
                        df = df[is_numeric.isin(sidebar_months)]
                    except:
                        pass
            
            # Well Filter
            if "Poço" in df.columns:
                unique_wells = sorted(df["Poço"].dropna().astype(str).unique())
                sel_wells = st.sidebar.multiselect("Filtrar Poço", unique_wells)
                if sel_wells:
                    df = df[df["Poço"].isin(sel_wells)]

            st.divider()
            
            # Show summary
            col1, col2 = st.columns([1, 3])
            col1.metric("Total Registros", f"{len(df):,}")
            if st.session_state.get('campos_selecionados'):
                col2.info(f"Campos: {', '.join(st.session_state['campos_selecionados'])}")
            
            st.dataframe(df)
            
            st.markdown("### Exportação")
            if not df.empty:
                excel_bytes = to_excel(df)
                st.download_button(
                    label="📥 Baixar Planilha Excel (.xlsx)",
                    data=excel_bytes,
                    file_name=f"Producao_ANP_{st.session_state['env']}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    
    elif cached_campos:
        st.info("👈 Selecione Campos na barra lateral e clique em 'Carregar Dados'.")

if __name__ == "__main__":
    main()
