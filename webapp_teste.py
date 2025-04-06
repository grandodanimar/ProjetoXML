import streamlit as st
import pandas as pd
import io
from io import BytesIO
#from io import StringIO
import xml.etree.ElementTree as ET

# Namespace do XML
ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def exporta_xml(files):

    dataframes = []

    # Iterar pelos arquivos carregados
    for arquivo in files:
        if arquivo.name.endswith(".xml"):  # Filtra apenas arquivos XML
            # Lendo os arquivos diretamente da memória
            tree = ET.parse(io.BytesIO(arquivo.read()))
            root = tree.getroot()

            # Inicializar variáveis gerais da nota fiscal
            chave_acesso = numero_nota = nat_op = data_emissao = None
            emitente_nome = cnpj_emitente = uf_emitente = destinatario_nome = cnpj_destinatario = uf_destinatario = None

            # Capturar a chave de acesso
            protNFe = root.find('.//nfe:protNFe/nfe:infProt', ns)
            if protNFe is not None:
                chave_acesso = protNFe.find('nfe:chNFe', ns).text if protNFe.find('nfe:chNFe', ns) is not None else None

            # Capturar os dados gerais da NF
            ide = root.find('.//nfe:infNFe/nfe:ide', ns)
            if ide is not None:
                numero_nota = ide.find('nfe:nNF', ns).text if ide.find('nfe:nNF', ns) is not None else None
                nat_op = ide.find('nfe:natOp', ns).text if ide.find('nfe:natOp', ns) is not None else None
                data_emissao = ide.find('nfe:dhEmi', ns).text if ide.find('nfe:dhEmi', ns) is not None else None

            # Capturar os dados do emitente
            emit = root.find('.//nfe:infNFe/nfe:emit', ns)
            if emit is not None:
                emitente_nome = emit.find('nfe:xNome', ns).text if emit.find('nfe:xNome', ns) is not None else None
                cnpj_emitente = emit.find('nfe:CNPJ', ns).text if emit.find('nfe:CNPJ', ns) is not None else None

            # Dados do endereço do emitente
            enderEmit = root.find('.//nfe:emit/nfe:enderEmit', ns)
            if enderEmit is not None:
                uf_emitente = enderEmit.find('nfe:UF', ns).text if enderEmit.find('nfe:UF', ns) is not None else None


            # Capturar os dados do destinatário
            dest = root.find('.//nfe:infNFe/nfe:dest', ns)
            if dest is not None:
                destinatario_nome = dest.find('nfe:xNome', ns).text if dest.find('nfe:xNome', ns) is not None else None
                cnpj_destinatario = dest.find('nfe:CNPJ', ns).text if dest.find('nfe:CNPJ', ns) is not None else None

            # Dados do endereço do emitente
            enderDest = root.find('.//nfe:dest/nfe:enderDest', ns)
            if enderDest is not None:
                uf_destinatario = enderDest.find('nfe:UF', ns).text if enderDest.find('nfe:UF', ns) is not None else None

            # Capturar os dados de cada produto
            for det in root.findall('.//nfe:det', ns):
                produtos = det.find('nfe:prod', ns)
                if produtos is not None:
                    codProd = produtos.find('nfe:cProd', ns).text if produtos.find('nfe:cProd', ns) is not None else None
                    cod_ean = produtos.find('nfe:cEAN', ns).text if produtos.find('nfe:cEAN', ns) is not None else None
                    nome_produto = produtos.find('nfe:xProd', ns).text if produtos.find('nfe:xProd', ns) is not None else None
                    ncm = produtos.find('nfe:NCM', ns).text if produtos.find('nfe:NCM', ns) is not None else None
                    cest = produtos.find('nfe:CEST', ns).text if produtos.find('nfe:CEST', ns) is not None else None
                    cfop = produtos.find('nfe:CFOP', ns).text if produtos.find('nfe:CFOP', ns) is not None else None
                    qtdeTrib = produtos.find('nfe:qTrib', ns).text if produtos.find('nfe:qTrib', ns) is not None else None
                    vlrUnit = produtos.find('nfe:vUnTrib', ns).text if produtos.find('nfe:vUnTrib', ns) is not None else None
                    vprod = produtos.find('nfe:vProd', ns).text if produtos.find('nfe:vProd', ns) is not None else None
                    vdesc = produtos.find('nfe:vDesc', ns).text if produtos.find('nfe:vDesc', ns) is not None else None

                    # Capturar os dados dos impostos (ICMS e PIS)
                    imposto = det.find('nfe:imposto', ns)
                    cst_icms = origem_prod = vBC_icms_prop = pICMS_prop = vICMS_prop = None
                    if imposto is not None:
                        icms00 = imposto.find('nfe:ICMS/nfe:ICMS00', ns)
                        if icms00 is not None:
                            origem_prod = icms00.find('nfe:orig', ns).text if icms00.find('nfe:orig', ns) is not None else None
                            cst_icms = icms00.find('nfe:CST', ns).text if icms00.find('nfe:CST', ns) is not None else None
                            vBC_icms_prop = icms00.find('nfe:vBC', ns).text if icms00.find('nfe:vBC', ns) is not None else None
                            pICMS_prop = icms00.find('nfe:pICMS', ns).text if icms00.find('nfe:pICMS', ns) is not None else None
                            vICMS_prop = icms00.find('nfe:vICMS', ns).text if icms00.find('nfe:vICMS', ns) is not None else None

                    cst_icms10 = origem_prod10 = pICMS = vICMS = vBC_icms10 = pICMS10 = vICMS10 = pMVA_ST = None

                    if imposto is not None:
                        icms10 = imposto.find('nfe:ICMS/nfe:ICMS10', ns) if imposto is not None else None
                        if icms10 is not None:
                            origem_prod10 = icms10.find('nfe:orig', ns).text if icms10.find('nfe:orig',ns) is not None else None
                            cst_icms10 = icms10.find('nfe:CST', ns).text if icms10.find('nfe:CST', ns) is not None else None
                            vBC_icms10 = icms10.find('nfe:vBCST', ns).text if icms10.find('nfe:vBCST', ns) is not None else None
                            vBC_icms = icms10.find('nfe:vBC', ns).text if icms10.find('nfe:vBC', ns) is not None else None
                            pICMS = icms10.find('nfe:pICMS', ns).text if icms10.find('nfe:pICMS', ns) is not None else None
                            vICMS = icms10.find('nfe:vICMS', ns).text if icms10.find('nfe:vICMS', ns) is not None else None
                            pICMS10 = icms10.find('nfe:pICMSST', ns).text if icms10.find('nfe:pICMSST', ns) is not None else None
                            vICMS10 = icms10.find('nfe:vICMSST', ns).text if icms10.find('nfe:vICMSST', ns) is not None else None
                            pMVA_ST = icms10.find('nfe:pMVAST', ns).text if icms10.find('nfe:pMVAST', ns) is not None else None


                    origem_prod61 = cst_icms61 = vBC_icms61 = pICMS61 = vICMS61 = None

                    if imposto is not None:
                        icms61 = imposto.find('nfe:ICMS/nfe:ICMS61', ns) if imposto is not None else None
                        if icms61 is not None:
                            origem_prod61 = icms61.find('nfe:orig', ns).text if icms61 is not None else None
                            cst_icms61 = icms61.find('nfe:CST', ns).text if icms61 is not None else None
                            vBC_icms61 = icms61.find('nfe:qBCMonoRet', ns).text if icms61 is not None else None
                            pICMS61 = icms61.find('nfe:adRemICMSRet', ns).text if icms61 is not None else None
                            vICMS61 = icms61.find('nfe:vICMSMonoRet', ns).text if icms61 is not None else None


                    # Capturar os dados do Pis
                    cst_pis = vBC_pis = pPIS = vPIS = cst_cofins = vBC_cofins = pCOFINS = vCOFINS = None
                    cst_pis_nt = vBC_pis_nt = pPIS_nt = vPIS_nt = cst_cofins_nt = vBC_cofins_nt = pCOFINS_nt = vCOFINS_nt = None

                    if imposto is not None:

                        pisaliq = imposto.find('nfe:PIS/nfe:PISAliq', ns) if imposto is not None else None
                        if pisaliq is not None:
                            cst_pis = pisaliq.find('nfe:CST', ns).text if pisaliq is not None else None
                            vBC_pis = pisaliq.find('nfe:vBC',ns).text if pisaliq is not None else None
                            pPIS = pisaliq.find('nfe:pPIS', ns).text if pisaliq is not None else None
                            vPIS = pisaliq.find('nfe:vPIS', ns).text if pisaliq is not None else None

                        Cofinsaliq = imposto.find('nfe:COFINS/nfe:COFINSAliq', ns) if imposto is not None else None
                        if Cofinsaliq is not None:
                            cst_cofins = Cofinsaliq.find('nfe:CST', ns).text if Cofinsaliq is not None else None
                            vBC_cofins = Cofinsaliq.find('nfe:vBC',ns).text if Cofinsaliq is not None else None
                            pCOFINS = Cofinsaliq.find('nfe:pCOFINS', ns).text if Cofinsaliq is not None else None
                            vCOFINS = Cofinsaliq.find('nfe:vCOFINS', ns).text if Cofinsaliq is not None else None

                        pisnt = imposto.find('nfe:PIS/nfe:PISNT', ns) if imposto is not None else None
                        if pisnt is not None:
                            cst_pis_nt = pisnt.find('nfe:CST', ns).text if pisnt is not None else None

                        cofinsnt = imposto.find('nfe:COFINS/nfe:COFINSNT', ns) if imposto is not None else None
                        if cofinsnt is not None:
                            cst_cofins_nt = cofinsnt.find('nfe:CST', ns).text if cofinsnt is not None else None

                    # Adicionar o produto como um registro ao dataframes
                    dataframes.append({
                        'Nat_Operacao': nat_op,
                        'Chave_Acesso': chave_acesso,
                        'Numero_NF': numero_nota,
                        'Data_Emissao': data_emissao,
                        'CNPJ_Emitente': cnpj_emitente,
                        'Nome_Emitente': emitente_nome,
                        'UF_Emitente': uf_emitente,
                        'CNPJ_Destinatário': cnpj_destinatario,
                        'Nome_Destinatário': destinatario_nome,
                        'UF_Destinatário': uf_destinatario,
                        'Código_EAN': cod_ean,
                        'Nome_Produto': nome_produto,
                        'NCM': ncm,
                        'CFOP': cfop,
                        'CEST': cest,
                        'Origem_Produto': origem_prod,
                        'CST_ICMS': cst_icms,
                        'Cod_Prod': codProd,
                        'Qtde_Trib':qtdeTrib,
                        'Vlr_Unit': vlrUnit,
                        'Vlr_Produto': vprod,
                        'Vlr_Desconto': vdesc,
                        'BC_ICMS': vBC_icms_prop,
                        'Per_ICMS': pICMS_prop,
                        'vlr_ICMS': vICMS_prop,
                        'Orig_Prod10': origem_prod10,
                        'CST_ICMS10': cst_icms10,
                        'BC_ICMS10': vBC_icms10,
                        'Per_ICMS10': pICMS10,
                        'vlr_ICMS10': vICMS10,
                        'Per_MVAST': pMVA_ST,
                        'Orig_Prod61': origem_prod61,
                        'BC_ICMS61': vBC_icms61,
                        'Per_ICMS61': pICMS61,
                        'vlr_ICMS61': vICMS61,
                        'CST_PIS': cst_pis,
                        'BC_Pis': vBC_pis,
                        'Per_Pis': pPIS,
                        'Vlr_Pis': vPIS,
                        'CST_COFINS': cst_cofins,
                        'BC_Cofins': vBC_cofins,
                        'Per_Cofins': pCOFINS,
                        'Vlr_Cofins': vCOFINS,
                        'CST_PIS_NT': cst_pis_nt,
                        'CST_COFINS_NT': cst_cofins_nt
                    })

    # Criar um DataFrame com os dados de todos os arquivos
    df = pd.DataFrame(dataframes)

    # Converter colunas numéricas
    cols_num_float = ['BC_ICMS','BC_ICMS61','vlr_ICMS61' , 'Per_ICMS61','vlr_ICMS','BC_ICMS10', 'vlr_ICMS10', 'Vlr_Produto','BC_Pis', 'Per_Pis','Vlr_Pis','BC_Cofins',
                  'Per_Cofins', 'Vlr_Cofins', 'Per_MVAST','Vlr_Unit', 'Qtde_Trib', 'Vlr_Desconto']
    df[cols_num_float]= df[cols_num_float].apply(lambda col: pd.to_numeric(col, errors='coerce').round(2))

    cols_int = ['Per_ICMS','Per_ICMS10']
    df[cols_int] = df[cols_int].apply(pd.to_numeric, errors='coerce', downcast='integer')

    # Converter coluna de data
    df['Data_Emissao'] = pd.to_datetime(df['Data_Emissao'], utc=True).dt.strftime('%Y-%m-%d')

    # Na conversão anterior a coluna continuará sendo string, portanto precisamos fazer a conversão novamente, porém sem dt.strftime()
    df['Data_Emissao'] = pd.to_datetime(df['Data_Emissao'], utc=True).dt.tz_convert(None)

    # o DataFrame apresenta valores nulos ou ausentes apenas nas colunas numéricas, desta forma preencheremos os mesmos com "0"
    df = df.fillna(0)

    # Inserindo novas colunas para Base de cálculo do ICMS e Valor do ICMS, concatenando as colunas com ICMS61 e ICMS
    df['BC_ICMS'] = np.where(df['BC_ICMS'] ==0, df['BC_ICMS'] + df['BC_ICMS61'], df['BC_ICMS'])
    df['Per_ICMS'] = df['Per_ICMS'] + df['Per_ICMS61']
    df['vlr_ICMS'] = df['vlr_ICMS'] + df['vlr_ICMS61']

    # Calcular um valor na coluna BC_Pis e Vlr_Pis subtraindo o valor da coluna VLR_ICMS da coluna bc_icms
    df['BC_PIS_Calc'] = df['Vlr_Produto'] - df['vlr_ICMS']
    df['VLR_PIS_Calc'] = df['BC_PIS_Calc'] * 0.0165

    df['BC_Cofins_Calc'] = df['Vlr_Produto'] - df['vlr_ICMS']
    df['VLR_Cofins_Calc'] = df['BC_Cofins_Calc'] * 0.076

    # Converte as novas colunas
    cols_pis_cofins = ['BC_PIS_Calc', 'VLR_PIS_Calc', 'BC_Cofins_Calc', 'VLR_Cofins_Calc']
    df[cols_pis_cofins] = df[cols_pis_cofins].apply(lambda col: pd.to_numeric(col, errors='coerce').round(2))

    # Mostrar DF
    return df
    

# Título da Página
st.markdown(
    """
    <div style="text-align": center;">
        <h1>Ferramenta de Leitura e Exportação de arquivos xml</h1>
    </div>
    """,
    unsafe_allow_html=True,
    )
# Upload de múltiplos arquivos
uploaded_files = st.file_uploader("Faça upload de arquivos XML", type="xml", accept_multiple_files=True)

if uploaded_files:
    df = exporta_xml(uploaded_files)
    
    # Salvar o DataFrame na sessão para uso posterior
    st.session_state.df = df


# Se o DataFrame já estiver carregado, exibir a opção de selecionar colunas
if "df" in st.session_state and st.session_state.df is not None:
    df = st.session_state.df
    colunas_disponiveis = df.columns.tolist()

    # Seleção de colunas com valores padrão (todas selecionadas inicialmente)
    # Com sidebar mostramos as colunas à esquerda da página, proporcionando uma interface mais organizada
    colunas_selecionadas = st.sidebar.multiselect(
        "Selecione as colunas para exibir:", colunas_disponiveis, default=colunas_disponiveis
    )

    # Exibir DataFrame com colunas selecionadas
    if colunas_selecionadas:
        st.write("### Dados do XML convertidos para DataFrame:")
        st.dataframe(df[colunas_selecionadas])
    else:
        st.warning("Selecione pelo menos uma coluna para exibição.")   

    # Converter para CSV e permitir download
    csv = df[colunas_selecionadas].to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Baixar CSV",
        data=csv,
        file_name="dados.csv",
        mime="text/csv"
    )

    # Converter para XLSX
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df[colunas_selecionadas].to_excel(writer, index=False, sheet_name="Planilha1")
        writer.book.close()

    st.download_button(
        label="Baixar XLSX(Excel)",
        data=output.getvalue(),
        file_name="dados.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
