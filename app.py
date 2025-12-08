import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import io
import re

# --- CONFIG ---
PAGE_TITLE = "ANP Produção de Petróleo e Gás"
DATA_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/fase-de-desenvolvimento-e-producao"

# --- HELPER FUNCTIONS ---

def get_available_years():
    """
    Scrapes the ANP website to find available years and their CSV links 
    for 'Produção em mar' (Offshore).
    
    Returns:
        dict: {year (int): url (str)}
    """
    try:
        response = requests.get(DATA_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # We are looking for links that usually look like "2024", "2023", etc.
        # The structure inspected showed they are often in lists found under headers.
        # We will look for all links that match a year pattern and contain 'csv' in href.
        # Refined strategy based on inspection: Look for links with text being a year.
        
        years_links = {}
        
        # Strategies to find specific 'Produção em mar' links could be complex 
        # without precise selectors. 
        # Heuristic: Find all links where text is a Year (YYYY) and href ends in .csv
        # Ideally, we would look for the section 'Produção em mar', but let's try a 
        # slightly broader approach filtered by keywords in URL if possible, or just exact year matches.
        
        for link in soup.find_all('a', href=True):
            text = link.get_text().strip()
            href = link['href']
            
            # Check if text is a year like "2025" OR a range like "1941-1979"
            # Regex for YYYY or YYYY-YYYY
            if re.match(r'^\d{4}(-\d{4})?$', text) and '.csv' in href.lower():
                # Prefer 'mar' (offshore) links if multiple exist for a year, 
                # but usually the year text is unique per section in the list.
                # Based on the inspection, the yearly links we saw were:
                # "2025" -> .../producao-mar-2025.csv
                # So if we find a year link that points to a CSV, it's a good candidate.
                # We can filter for 'mar' in href to be safe if accessible.
                
                if 'mar' in href.lower() or 'producao_por_poco' in href.lower():
                     # Store as string to handle "1941-1979"
                     years_links[text] = href

        # Sort descending (works for strings: "2025" > "1941...")
        return dict(sorted(years_links.items(), key=lambda item: item[0], reverse=True))

    except Exception as e:
        st.error(f"Erro ao buscar dados do site: {e}")
        return {}

def process_dataframe(df):
    """
    Cleans and processes the DataFrame:
    - Converts numerical columns from Brazilian format (1.234,56) to float (1234.56).
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
    
    # Ensure they exist (intersection)
    valid_cols = [c for c in cols_to_convert if c in df.columns]
    
    for col in valid_cols:
        # Check if already numeric
        if not pd.api.types.is_numeric_dtype(df[col]):
            # Remove thousand separators (.) and replace decimal (,) with (.)
            try:
                df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            except Exception:
                pass
    
    # Remove unwanted columns
    cols_to_drop = [
        "Bacia", 
        "Instalação", 
        "Estado", 
        "Ambiente", 
        "Produção de Condensado (m³)", 
        "Injeção de Polímeros (m³)", 
        "Injeção de Outros Fluidos (m³)"
    ]
    # Drop only those that exist
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')
                
    return df

@st.cache_data(show_spinner=True)
def load_data(url):
    """
    Downloads CSV from URL, parses it, and caches the result.
    """
    try:
        # Based on inspection:
        # - Separator is ','
        # - Headers are like [Ano], [Campo]
        # - Encoding is likely 'windows-1252' or 'latin1'
        
        response = requests.get(url)
        response.raise_for_status()
        csv_content = io.BytesIO(response.content)
        
        # Try reading
        try:
             df = pd.read_csv(csv_content, sep=',', encoding='windows-1252', on_bad_lines='skip')
        except UnicodeDecodeError:
             csv_content.seek(0)
             df = pd.read_csv(csv_content, sep=',', encoding='utf-8', on_bad_lines='skip')
             
        # CLEAN HEADERS: Remove brackets [] and whitespace
        # Example: "[Campo]" -> "Campo"
        df.columns = df.columns.str.replace(r'[\[\]]', '', regex=True).str.strip()
             
        df = process_dataframe(df)
        return df
        
    except Exception as e:
        st.error(f"Erro ao carregar CSV: {e}")
        return pd.DataFrame()

def to_excel(df):
    """
    Converts DataFrame to Excel bytes.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        
        # Get dimensions
        (max_row, max_col) = df.shape
        
        # Create a table
        column_settings = [{'header': column} for column in df.columns]
        worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
        
        # Auto-adjust columns (rough estimate)
        worksheet.set_column(0, max_col - 1, 15)
        
    return output.getvalue()

# --- MAIN APP ---

def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)
    
    st.sidebar.header("Configurações")
    
    # 1. Select Year (Scrape available or Fallback)
    with st.spinner("Buscando anos disponíveis no site da ANP..."):
        available_years = get_available_years()
    
    if available_years:
        selected_year = st.sidebar.selectbox("Selecione o Ano", options=list(available_years.keys()))
        csv_url = available_years[selected_year]
        st.sidebar.info(f"Fonte: Site da ANP (Ano {selected_year})")
    else:
        st.warning("Não foi possível carregar os anos automaticamente. Insira o link manual se desejar.")
        csv_url = st.sidebar.text_input("URL do CSV")
        selected_year = None

    # 2. Download Button
    if st.sidebar.button("Baixar/Atualizar Dados da ANP"):
        if csv_url:
            st.session_state['data'] = load_data(csv_url)
            st.session_state['year'] = selected_year
        else:
            st.error("Nenhuma URL válida selecionada.")

    # 3. Main Data View
    if 'data' in st.session_state and not st.session_state['data'].empty:
        df = st.session_state['data']
        
        st.markdown(f"### Dados do Ano: {st.session_state.get('year', 'N/A')}")
        st.write(f"Total de registros carregados: {len(df)}")
        
        # Filters
        st.markdown("#### Filtros")
        col1, col2 = st.columns(2)
        
        # Determine strict column names for filtering
        # The cleaning step guarantees "Campo" and "Poço" if they were "[Campo]" and "[Poço]"
        
        filtered_df = df.copy()
        
        # Filter by Campo
        if "Campo" in filtered_df.columns:
            campos = sorted(filtered_df["Campo"].dropna().astype(str).unique().tolist())
            selected_campos = col1.multiselect("Filtrar por Campo", options=campos)
            if selected_campos:
                filtered_df = filtered_df[filtered_df["Campo"].isin(selected_campos)]
        else:
            col1.warning("Coluna 'Campo' não encontrada.")
            
        # Filter by Poço
        if "Poço" in filtered_df.columns:
            # Dependent on Campo selection? Usually yes, but here we just filter the current df
            pocos = sorted(filtered_df["Poço"].dropna().astype(str).unique().tolist())
            selected_pocos = col2.multiselect("Filtrar por Poço", options=pocos)
            if selected_pocos:
                filtered_df = filtered_df[filtered_df["Poço"].isin(selected_pocos)]
        else:
            col2.warning("Coluna 'Poço' não encontrada.")
        
        # Show count
        st.info(f"Exibindo {len(filtered_df)} registros.")

        # Preview
        st.dataframe(filtered_df, use_container_width=True)
        
        # Export
        st.markdown("---")
        if not filtered_df.empty:
            excel_data = to_excel(filtered_df)
            st.download_button(
                label="📥 Gerar Relatório Excel",
                data=excel_data,
                file_name=f"producao_anp_{st.session_state.get('year', 'dados')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("Sem dados para exportar com os filtros atuais.")
            
    elif 'data' in st.session_state:
        st.warning("O arquivo foi baixado mas parece estar vazio ou inválido.")
    else:
        st.info("Utilize o menu lateral para selecionar o ano e baixar os dados.")

if __name__ == "__main__":
    main()

