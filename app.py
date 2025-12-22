import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import io
import re
import numpy as np
import xlsxwriter

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

# --- MAIN APP ---

def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)

    st.sidebar.header("Configurações")

    # 1. Scraping Years
    with st.spinner("Conectando ao site da ANP..."):
        annotated_years = get_available_years()

    if annotated_years:
        # Year Selection (Multi-select)
        available_years = list(annotated_years.keys())
        selected_years = st.sidebar.multiselect(
            "1. Selecione o(s) Ano(s)",
            options=available_years,
            placeholder="Escolha um ou mais anos"
        )

        # Environment Selection
        # Logic: Show envs available in ANY of the selected years? Or intersection?
        # Simpler: Show Terra/Mar and warn if missing.
        selected_env = st.sidebar.radio("2. Selecione o Ambiente", ["Terra", "Mar"])

        # Determine URLs to download
        urls_to_download = []
        missing_years_for_env = []

        if selected_years:
            for y in selected_years:
                if y in annotated_years and selected_env in annotated_years[y]:
                    urls_to_download.extend(annotated_years[y][selected_env])
                else:
                    missing_years_for_env.append(y)

        if selected_years:
            st.sidebar.info(f"Arquivos: {len(urls_to_download)} (de {len(selected_years)} anos)")
            if missing_years_for_env:
                st.sidebar.warning(f"Sem dados {selected_env}: {', '.join(missing_years_for_env)}")

        # Download Button
        if st.sidebar.button("Baixar Dados"):
            if urls_to_download:
                st.session_state['data'] = get_dataset(urls_to_download)
                st.session_state['year'] = ", ".join(sorted(selected_years))
                st.session_state['env'] = selected_env
            else:
                if not selected_years:
                    st.error("Selecione pelo menos um ano.")
                else:
                    st.error("Nenhum arquivo encontrado para a seleção.")

    else:
        st.error("Não foi possível carregar a lista de anos do site da ANP.")
        st.warning("Verifique sua conexão ou se o site da ANP mudou de estrutura.")

    # 3. Data View
    if 'data' in st.session_state and not st.session_state['data'].empty:
        df = st.session_state['data']

        st.sidebar.markdown("---")
        st.sidebar.header("3. Filtros Globais")

        # 3.1 Sidebar Month Filter
        # Identify Month Column (Mês or Mes)
        month_col = None
        for col in ['Mês', 'Mes', 'Month']:
            if col in df.columns:
                month_col = col
                break

        if month_col:
            # Map for display 1 -> Janeiro
            month_map = {
                1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
                7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
            }

            # User wants ALL months available (1-12), not just what's in DF
            all_months = list(range(1, 13))

            # Helper to format
            def format_month(m):
                return f"{month_map.get(m, m)} ({m})"

            sidebar_months = st.sidebar.multiselect(
                "Filtrar Mês",
                options=all_months,
                format_func=format_month,
                placeholder="Selecione os meses"
            )

            if sidebar_months:
                # Ensure column is numeric for comparison
                # Try to convert column to numeric if it isn't already, for reliable filtering
                try:
                    # Create a temporary mask without modifying original df inplace immediately if possible
                    # But keeping simple: force numeric conversion for the filter column
                    # If it fails (non-numeric data), we might miss rows, but ANP data usually is clean or cleaned
                    is_numeric = pd.to_numeric(df[month_col], errors='coerce')
                    mask = is_numeric.isin(sidebar_months)
                    df = df[mask]
                except Exception:
                    # Fallback: compare as strings if numeric conversion fails completely
                    sidebar_months_str = [str(m) for m in sidebar_months]
                    df = df[df[month_col].astype(str).isin(sidebar_months_str)]

        # 3.2 Sidebar Field Filter
        if "Campo" in df.columns:
            campos_sb = sorted(df["Campo"].dropna().astype(str).unique())
            sel_campos_sb = st.sidebar.multiselect("Filtrar Campo", campos_sb)
            if sel_campos_sb:
                df = df[df["Campo"].isin(sel_campos_sb)]

        # 3.3 Sidebar Well Filter
        if "Poço" in df.columns:
            pocos_sb = sorted(df["Poço"].dropna().astype(str).unique())
            sel_pocos_sb = st.sidebar.multiselect("Filtrar Poço", pocos_sb)
            if sel_pocos_sb:
                df = df[df["Poço"].isin(sel_pocos_sb)]

        st.divider()
        st.markdown(f"### 📊 Análise: {st.session_state['year']} - {st.session_state['env']}")
        st.write(f"**Total de Registros:** {len(df):,}")

        # --- MAIN VIEW ---
        # Filters are now exclusively in the sidebar as requested

        st.dataframe(df, use_container_width=True)

        # --- EXPORT ---
        st.markdown("### Exportação")
        if not df.empty:
            excel_bytes = to_excel(df)
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

