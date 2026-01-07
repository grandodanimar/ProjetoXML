import streamlit as st
from PyPDF2 import PdfMerger
import io

def main():
    st.title("Manipular PDFs")

    uploaded_files = st.file_uploader(
        "Selecione os PDFs",
        type="pdf",
        accept_multiple_files=True
    )

    if uploaded_files:
        merger = PdfMerger()

        for pdf in uploaded_files:
            merger.append(pdf)

        buffer = io.BytesIO()
        merger.write(buffer)
        merger.close()
        buffer.seek(0)

        st.success("PDFs combinados com sucesso!")

        st.download_button(
            label="📥 Baixar PDF combinado",
            data=buffer,
            file_name="pdf_combinado.pdf",
            mime="application/pdf"
        )

# 🔴 ISSO É OBRIGATÓRIO
if __name__ == "__main__":
    main()







