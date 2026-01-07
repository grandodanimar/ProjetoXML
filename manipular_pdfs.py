import streamlit as st
import os
from PyPDF2 import PdfMerger
from io import BytesIO

uploaded_files = st.file_uploader("Carregue aqui a pasta com seus arquivos pdf", type="pdf", accept_multiple_files=True, width=500)

# Função para combinar os pdfs

def combina_pdfs(uploaded_files):

	merger = PdfMerger()
	output = BytesIO()

	for pdf in uploaded_files:
		merger.append(pdf)

	merger.write('merged-pdf.pdf')
	merger.close()
	output.seek(0)

	return output

	if uploaded_files:
		if st.button("🔗 Combinar PDFs"):
			pdf_final = combina_pdfs(uploaded_files)
	
			st.success("PDFs combinados com sucesso!")
	
	
			st.download_button(
				label="⬇️ Baixar PDF combinado",
				data=pdf_final,
				file_name="arquivos_combinados",
				mime="application/pdf"
			)
		
if __name__ == "__main__":
    combina_pdfs()





