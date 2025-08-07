import streamlit as st

# Acessar variáveis principais
username = st.secrets["DB_USERNAME"]
token = st.secrets["DB_TOKEN"]

# Acessar dados dentro de uma seção
some_key_value = st.secrets["some_section"]["some_key"]

st.write(f"Usuário: {username}")
st.write(f"Chave: {some_key_value}")

pagina_1 = st.Page("2-leitor_xml_com_duckdb.py", title="Converter Arquivos XML para Tabela")
pagina_2 = st.Page("3-leitor_pdf.py", title="Conferência Autorizações de Uso NFe")
pg = st.navigation([pagina_1, pagina_2])
pg.run()

