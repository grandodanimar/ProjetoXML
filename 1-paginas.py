import streamlit as st
st.set_page_config(
	page_title="Conversor XML",
    layout="wide",
	page_icon=":memo:"
)

paginas = {
	
	"Ferramentas":[
		#st.Page("autenticacao.py", title="Login"),
		st.Page("2-leitor_xml_com_duckdb.py", title="Converter Arquivos XML para Tabela"),
		st.Page("3-leitor_pdf.py", title="Lendo Arquivos PDF")
		#st.Page("manipular_pdfs.py", title="Meu PDF")
		],
}

pg = st.navigation(paginas)
with st.sidebar.expander("**Dúvidas e Sugestões**"):
    st.write('''
        Envie sugestões ou tire dúvidas no email abaixo:

        danimar.grando@outlook.com
        
    ''')
pg.run()


















