import streamlit as st

pagina_1 = st.Page("2-leitor_xml_com_duckdb.py", title="Converter Arquivos XML para Tabela")
pagina_2 = st.Page("3-leitor_pdf.py", title="Conferência Autorizações de Uso NFe")
pg = st.navigation([pagina_1, pagina_2])
pg.run()
