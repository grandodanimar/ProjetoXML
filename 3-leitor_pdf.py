import numpy as np
from PyPDF2 import PdfReader
import pandas as pd
import re
import os
import streamlit as st


#Função para limpar valores de uma coluna
def limpa_valor(val):
    if isinstance(val, list) and val:
        val = val[0]
    if isinstance(val, float) or isinstance(val, int):
        return val
    if isinstance(val, str):
        val = val.replace('.', '').replace(',', '.').replace(' ', '')
        try:
            return float(val)
        except ValueError:
            return None
    return None



def carrega_pdf(files):
    # Cria uma lista para armazenar os dados extraídos
	dados = []

	# Iterar pelos arquivos no diretório
	for arquivo in files:
	    if arquivo.name.endswith(".pdf"):  # Filtra apenas arquivos pdf	        
            
            # O reader vai ler cada arquivo carregado
	        reader = PdfReader(arquivo)
	        arquivo = reader.pages[0]
	        texto = arquivo.extract_text()
	        #print("Arquivos encontrados:", texto)
	        regex_chave = r"Chave de acesso[\s\S]*?((?:\d[\s\-\/\.]*){44})"
	        nro_nfe = r"Número NF-e\s+(\w+.+)\s*Série"
	        data_emissao = r"Data de emissão\s+(\w+.+)\s*-"
	        data_impressao = r"Data/Hora Impressão:\s+(\w+.+)\s*Válida "
	        nome_emitente = r"Razão Social do Emitente\s+(\w+.+)\s*CNPJ do Emitente"
	        cnpj_emitente = r"CNPJ do Emitente\s+(\w+.+)\s*UF"
	        nome_destinatario = r"Razão Social do Destinatário\s+(\w+.+)\s*CNPJ do Destinatário"
	        cnpj_destinatario = r"CNPJ do Destinatário\s+(\w+.+)\s*UF"
	        base_calc_icms = r"Base de cálculo do ICMS\s+(\w+.+)\s*Valor do ICMS"
	        vlr_icms = r"Valor do ICMS\s+(\w+.+)\s*Valor Total da NF-e"
	        vlr_total_nfe = r"Valor Total da NF-e\s+(\w+.+)\s*Data/Hora Impressão: "

	        dados.append({
	        	'Chave_Acesso': re.findall(regex_chave, texto),
	            'Numero_NF': re.findall(nro_nfe, texto),
	            'Data_Emissao': re.findall(data_emissao, texto),
	            'Data_Impressao': re.findall(data_impressao, texto),
	            'Nome_Emitente': re.findall(nome_emitente, texto),
	            'CNPJ_Emitente': re.findall(cnpj_emitente, texto),
	            'Nome_Destinatario':re.findall(nome_destinatario, texto),
	            'CNPJ_Destinatario': re.findall(cnpj_destinatario, texto),
	            'BC_Icms': re.findall(base_calc_icms, texto),
	            'Vlr_Icms': re.findall(vlr_icms, texto),
	            'Vlr_Total_Nfe': re.findall(vlr_total_nfe, texto)

	        })

	 # Convertendo a lista dos valores encontrados em Dataframe
	dataframe = pd.DataFrame.from_dict(dados)
	

	# Tratamento dos dados
	dataframe['Chave_Acesso'] = dataframe['Chave_Acesso'].str[0]
	# Limpar dados diferentes de números da chave de acesso
	dataframe['Chave_Acesso'] = dataframe['Chave_Acesso'].str.replace(".", "").str.replace("-", "").str.replace(" ", "").str.replace("/", "").str.strip()
	dataframe['Numero_NF'] = dataframe['Numero_NF'].apply(limpa_valor)
	dataframe['Data_Emissao'] = dataframe['Data_Emissao'].str[0]
	dataframe['Data_Impressao'] = dataframe['Data_Impressao'].str[0]
	dataframe['Nome_Emitente'] = dataframe['Nome_Emitente'].str[0]
	dataframe['CNPJ_Emitente'] = dataframe['CNPJ_Emitente'].str[0]
	dataframe['Nome_Destinatario'] = dataframe['Nome_Destinatario'].str[0]
	dataframe['CNPJ_Destinatario'] = dataframe['CNPJ_Destinatario'].str[0]
	dataframe['BC_Icms'] = dataframe['BC_Icms'].apply(limpa_valor)
	dataframe['Vlr_Icms'] = dataframe['Vlr_Icms'].apply(limpa_valor)
	dataframe['Vlr_Total_Nfe'] = dataframe['Vlr_Total_Nfe'].apply(limpa_valor)
	
	return dataframe

st.markdown(
    """
    <div style="text-align": center;">
        <h1>Ferramenta de Conferência de Autorizações de Uso</h1>
    </div>
    """,
    unsafe_allow_html=True,
    )

# Upload de múltiplos arquivos
uploaded_files = st.file_uploader("Faça upload de arquivos PDF", type="pdf", accept_multiple_files=True)
st.markdown("Atenção os arquivos de autorização de uso devem estar no formato abaixo para que sejam processadas corretamente!")
st.image("autorizacao_uso.png")

if uploaded_files:
    df = carrega_pdf(uploaded_files)
    
    # Salvar o DataFrame na sessão para uso posterior
    st.session_state.df = df
    st.dataframe(df.dropna(), hide_index=True)
    conta_docs = df['Chave_Acesso'].nunique()
    
    col1 = st.columns(1)[0]
    col1.metric("Total de Documentos Carregados", conta_docs)
    #col2.metric("Total de Documentos Válidos", conta_docs_validos)
    