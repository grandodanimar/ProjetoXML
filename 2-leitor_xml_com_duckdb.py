import streamlit as st
import pandas as pd
import numpy as np
import io
import duckdb
from io import BytesIO
#from io import StringIO
import xml.etree.ElementTree as ET

# Namespace do XML
ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

@st.cache_data
def exporta_xml(files):

    # Conecta ao banco de dados DuckDB na memória
    con = duckdb.connect(database=':memory:')

    # Cria uma tabela chamada 'notas_fiscais_xml'
    con.execute("""
                CREATE TABLE notas_fiscais_xml (
                Nat_Operacao VARCHAR, 
                Chave_Acesso VARCHAR, 
                Numero_NF INTEGER, 
                Data_Emissao DATE, 
                CNPJ_Emitente VARCHAR, 
                CPF_Emitente VARCHAR, 
                Nome_Emitente VARCHAR, 
                UF_Emitente TEXT, 
                CNPJ_Destinatário VARCHAR, 
                Nome_Destinatário VARCHAR, 
                UF_Destinatário TEXT, 
                Código_EAN VARCHAR, 
                Nome_Produto VARCHAR, 
                NCM TEXT, 
                CFOP INTEGER, 
                CEST TEXT, 
                cBenef TEXT, 
                cCredPresumido TEXT, 
                pCredPresumido FLOAT ,  
                vCredPresumido FLOAT, 
                Origem_Produto FLOAT, 
                CST_ICMS VARCHAR, 
                Cod_Prod VARCHAR, 
                Qtde_Trib FLOAT, 
                Vlr_Unit FLOAT, 
                Vlr_Produto FLOAT, 
                Vlr_Desconto FLOAT, 
                BC_ICMS FLOAT, 
                Per_ICMS FLOAT, 
                vlr_ICMS FLOAT, 
                CST_ICMS20 INTEGER, 
                Orig_Prod20 INTEGER, 
                Per_RedBC FLOAT, 
                BC_ICMS20 FLOAT, 
                Per_ICMS20 FLOAT, 
                Vlr_ICMS20 FLOAT, 
                Orig_Prod10 INTEGER, 
                CST_ICMS10 INTEGER, 
                BC_ICMS10 FLOAT, 
                Per_ICMS10 FLOAT, 
                Vlr_ICMS10 FLOAT, 
                Per_MVAST FLOAT, 
                BC_ICMS_ST FLOAT, 
                Per_ICMS_ST FLOAT, 
                Vlr_ICMS_ST FLOAT, 
                Orig_Prod51 INTEGER, 
                CST_ICMS51 INTEGER, 
                BC_ICMS51 FLOAT, 
                Per_ICMS51 FLOAT, 
                Per_Dif FLOAT, 
                Vlr_ICMS_Op FLOAT, 
                Vlr_ICMS_Dif FLOAT, 
                Vlr_ICMS51 FLOAT, 
                Orig_Prod61 INTEGER, 
                BC_ICMS61 FLOAT, 
                Per_ICMS61 FLOAT, 
                vlr_ICMS61 FLOAT,
                Orig_Prod90 INTEGER,
                CST_ICMS90 INTEGER,
                CST_IPI INTEGER,
                CST_IPI_NT INTEGER,
                BC_IPI FLOAT,
                Per_IPI FLOAT,
                Vlr_IPI FLOAT,
                CST_PIS INTEGER, 
                BC_Pis FLOAT, 
                Per_Pis FLOAT, 
                Vlr_Pis FLOAT, 
                CST_COFINS INTEGER, 
                BC_Cofins FLOAT, 
                Per_Cofins FLOAT, 
                Vlr_Cofins FLOAT, 
                CST_PIS_NT INTEGER, 
                CST_COFINS_NT INTEGER);
                """
    )

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
                cpf_emitente = emit.find('nfe:CPF', ns).text if emit.find('nfe:CPF', ns) is not None else None

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
                    cbenef = produtos.find('nfe:cBenef', ns).text if produtos.find('nfe:cBenef', ns) is not None else None

                    gcred = produtos.find('nfe:gCred', ns)

                    cCredPresumido=pCredPresumido=vCredPresumido=None
                    if gcred is not None:
                        cCredPresumido = gcred.find('nfe:cCredPresumido', ns).text if gcred.find('nfe:cCredPresumido', ns) is not None else None
                        pCredPresumido = gcred.find('nfe:pCredPresumido', ns).text if gcred.find('nfe:pCredPresumido', ns) is not None else None
                        vCredPresumido = gcred.find('nfe:vCredPresumido', ns).text if gcred.find('nfe:vCredPresumido', ns) is not None else None

                    # Capturar os dados dos impostos (ICMS e PIS)
                    imposto = det.find('nfe:imposto', ns)
                    cst_icms = origem_prod = vBC_icms_prop = pICMS_prop = vICMS_prop = None
                    cst_icms20 = origem_prod20 = per_redBC = vBC_icms20 = pICMS20 = vICMS20 = None
                    if imposto is not None:
                        icms00 = imposto.find('nfe:ICMS/nfe:ICMS00', ns)
                        if icms00 is not None:
                            origem_prod = icms00.find('nfe:orig', ns).text if icms00.find('nfe:orig', ns) is not None else None
                            cst_icms = icms00.find('nfe:CST', ns).text if icms00.find('nfe:CST', ns) is not None else None
                            vBC_icms_prop = icms00.find('nfe:vBC', ns).text if icms00.find('nfe:vBC', ns) is not None else None
                            pICMS_prop = icms00.find('nfe:pICMS', ns).text if icms00.find('nfe:pICMS', ns) is not None else None
                            vICMS_prop = icms00.find('nfe:vICMS', ns).text if icms00.find('nfe:vICMS', ns) is not None else None
                        icms20 = imposto.find('nfe:ICMS/nfe:ICMS20', ns)
                        if icms20 is not None:
                            origem_prod20 = icms20.find('nfe:orig', ns).text if icms20.find('nfe:orig', ns) is not None else None
                            cst_icms20 = icms20.find('nfe:CST', ns).text if icms20.find('nfe:CST', ns) is not None else None
                            per_redBC = icms20.find('nfe:pRedBC', ns).text if icms20.find('nfe:pRedBC', ns) is not None else None
                            vBC_icms20 = icms20.find('nfe:vBC', ns).text if icms20.find('nfe:vBC', ns) is not None else None
                            pICMS20 = icms20.find('nfe:pICMS', ns).text if icms20.find('nfe:pICMS', ns) is not None else None
                            vICMS20 = icms20.find('nfe:vICMS', ns).text if icms20.find('nfe:vICMS', ns) is not None else None
    
                    cst_icms10 = origem_prod10 = pICMS_st = vBC_icms_st = vICMS_st = vICMS = vBC_icms10 = pICMS10 = vICMS10 = pMVA_ST = None

                    if imposto is not None:
                        icms10 = imposto.find('nfe:ICMS/nfe:ICMS10', ns) if imposto is not None else None
                        if icms10 is not None:
                            origem_prod10 = icms10.find('nfe:orig', ns).text if icms10.find('nfe:orig',ns) is not None else None
                            cst_icms10 = icms10.find('nfe:CST', ns).text if icms10.find('nfe:CST', ns) is not None else None
                            vBC_icms_st = icms10.find('nfe:vBCST', ns).text if icms10.find('nfe:vBCST', ns) is not None else None
                            vBC_icms10 = icms10.find('nfe:vBC', ns).text if icms10.find('nfe:vBC', ns) is not None else None
                            pICMS10 = icms10.find('nfe:pICMS', ns).text if icms10.find('nfe:pICMS', ns) is not None else None
                            vICMS10 = icms10.find('nfe:vICMS', ns).text if icms10.find('nfe:vICMS', ns) is not None else None
                            pICMS_st = icms10.find('nfe:pICMSST', ns).text if icms10.find('nfe:pICMSST', ns) is not None else None
                            vICMS_st = icms10.find('nfe:vICMSST', ns).text if icms10.find('nfe:vICMSST', ns) is not None else None
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

                    origem_prod51 = cst_icms51 = vBC_icms51 = pICMS51 = per_Dif = vICMSop = vICMSdif = vICMS51=None

                    if imposto is not None:
                        icms51 = imposto.find('nfe:ICMS/nfe:ICMS51', ns) if imposto is not None else None
                        if icms51 is not None:
                            origem_prod51 = icms51.find('nfe:orig', ns).text if icms51 is not None else None
                            cst_icms51 = icms51.find('nfe:CST', ns).text if icms51 is not None else None
                            vBC_icms51 = icms51.find('nfe:vBC', ns).text if icms51 is not None else None
                            pICMS51 = icms51.find('nfe:pICMS', ns).text if icms51 is not None else None
                            per_Dif = icms51.find('nfe:pDif', ns).text if icms51 is not None else None
                            vICMSop = icms51.find('nfe:vICMSOp', ns).text if icms51 is not None else None
                            vICMSdif = icms51.find('nfe:vICMSDif', ns).text if icms51 is not None else None
                            vICMS51 = icms51.find('nfe:vICMS', ns).text if icms51 is not None else None
                    orig_prod90 = cst_icms90 = None
                    if imposto is not None:
                        icms90 = imposto.find('nfe:ICMS/nfe:ICMS90', ns) if imposto is not None else None
                        if icms90 is not None:
                            orig_prod90 = icms90.find('nfe:orig', ns).text if icms90 is not None else None
                            cst_icms90 = icms90.find('nfe:CST', ns).text if icms90 is not None else None      


                    # Capturar os dados do Pis
                    cst_pis = vBC_pis = pPIS = vPIS = cst_cofins = vBC_cofins = pCOFINS = vCOFINS = None
                    cst_pis_nt = vBC_pis_nt = pPIS_nt = vPIS_nt = cst_cofins_nt = vBC_cofins_nt = pCOFINS_nt = vCOFINS_nt = None
                    cst_ipi = cst_ipi_nt = vBC_IPI = pIPI = vIPI = None
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

                        ipint = imposto.find('nfe:IPI/nfe:IPINT', ns) if imposto is not None else None
                        if ipint is not None:
                            cst_ipi_nt = ipint.find('nfe:CST', ns).text if ipint is not None else None

                        ipitrib = imposto.find('nfe:IPI/nfe:IPITrib',ns) if imposto is not None else None
                        if ipitrib is not None:
                            cst_ipi = ipitrib.find('nfe:CST',ns).text if ipitrib is not None else None
                            vBC_IPI = ipitrib.find('nfe:vBC',ns)
                            vBC_IPI = vBC_IPI.text if vBC_IPI is not None else None
                            pIPI = ipitrib.find('nfe:pIPI',ns)
                            pIPI = pIPI.text if pIPI is not None else None
                            vIPI = ipitrib.find('nfe:vIPI',ns)
                            vIPI = vIPI.text if vIPI is not None else None

                    # Inserindo os dados no banco de dados criado em memória
                    con.execute("""
                    INSERT INTO notas_fiscais_xml VALUES (?,?,?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,
                    ?,?,?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?,?, ?, ?,?,?,?,?,?, ?,?,?,?,?)""", 
                    (nat_op,chave_acesso,numero_nota, data_emissao, cnpj_emitente,cpf_emitente,emitente_nome,uf_emitente,cnpj_destinatario,destinatario_nome,
                    uf_destinatario, cod_ean,nome_produto,ncm,cfop,cest,cbenef,cCredPresumido,pCredPresumido,vCredPresumido,origem_prod,cst_icms,codProd,
                    qtdeTrib,vlrUnit,vprod,vdesc,vBC_icms_prop,pICMS_prop,vICMS_prop,cst_icms20,origem_prod20,per_redBC,vBC_icms20,pICMS20,vICMS20,origem_prod10,
                    cst_icms10,vBC_icms10,pICMS10,vICMS10, pMVA_ST,vBC_icms_st,pICMS_st,vICMS_st,origem_prod51,cst_icms51,vBC_icms51,pICMS51,per_Dif, vICMSop,
                    vICMSdif,vICMS51,origem_prod61,vBC_icms61,pICMS61,vICMS61, orig_prod90, cst_icms90,cst_ipi, cst_ipi_nt,vBC_IPI,pIPI,vIPI, cst_pis,vBC_pis,pPIS,vPIS,cst_cofins,vBC_cofins,pCOFINS,vCOFINS,cst_pis_nt,cst_cofins_nt)
                    )


    df = con.execute("select * from notas_fiscais_xml").df()

    # Converter colunas numéricas
    cols_num_float = ['BC_ICMS','BC_ICMS61','vlr_ICMS61' , 'Per_ICMS61','vlr_ICMS','BC_ICMS10', 'Vlr_ICMS10', 'Vlr_Produto','BC_Pis', 'Per_Pis','Vlr_Pis','BC_Cofins',
                  'Per_Cofins', 'Vlr_Cofins', 'Per_MVAST','Vlr_Unit', 'Qtde_Trib', 'Vlr_Desconto', 'BC_ICMS51', 'Per_ICMS51', 'Per_Dif', 'Vlr_ICMS_Op', 'Vlr_ICMS_Dif','Vlr_ICMS51', 
                     'Per_RedBC', 'BC_ICMS20', 'Per_ICMS20', 'Vlr_ICMS20', 'Per_ICMS_ST', 'BC_ICMS_ST', 'Vlr_ICMS_ST','vCredPresumido', 'pCredPresumido']
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
    #df['BC_ICMS'] = np.where(df['BC_ICMS'] ==0, df['BC_ICMS'] + df['BC_ICMS61'] + df['BC_ICMS51'] +df['BC_ICMS20'], df['BC_ICMS']+ df['BC_ICMS10'])
    #df['Vlr_ICMS51'] = df['Vlr_ICMS_Op']-df['Vlr_ICMS_Dif']
    df['BC_ICMS'] = df['BC_ICMS'] + df['BC_ICMS61'] + df['BC_ICMS51'] +df['BC_ICMS20']+ df['BC_ICMS10']
    df['Per_ICMS'] = df['Per_ICMS'] + df['Per_ICMS61'] + df['Per_ICMS51'] + df['Per_ICMS20'] + df['Per_ICMS10']
    df['vlr_ICMS'] = df['vlr_ICMS'] + df['vlr_ICMS61'] + df['Vlr_ICMS51'] + df['Vlr_ICMS20'] + df['Vlr_ICMS10']

    # Calcular um valor na coluna BC_Pis e Vlr_Pis subtraindo o valor da coluna VLR_ICMS da coluna bc_icms
    df['BC_PIS_Calc'] = df['Vlr_Produto'] - df['vlr_ICMS'] - df['Vlr_Desconto']
    df['VLR_PIS_Calc'] = df['BC_PIS_Calc'] * 0.0165

    df['BC_Cofins_Calc'] = df['Vlr_Produto'] - df['vlr_ICMS'] - df['Vlr_Desconto']
    df['VLR_Cofins_Calc'] = df['BC_Cofins_Calc'] * 0.076

    # Converte as novas colunas
    cols_pis_cofins = ['BC_PIS_Calc', 'VLR_PIS_Calc', 'BC_Cofins_Calc', 'VLR_Cofins_Calc']
    df[cols_pis_cofins] = df[cols_pis_cofins].apply(lambda col: pd.to_numeric(col, errors='coerce').round(2))
    df['CPF_Emitente'] = df['CPF_Emitente'].astype(str)

    # Mostrar DF
    return df
    
st.title("")
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

    tab1, tab2, tab3 = st.tabs(["Tabela Completa", "Consulta por MVA", "Relatório PisCofins"])

    with tab1:
        
            colunas_disponiveis = df.columns.tolist()

            # Seleção de colunas com valores padrão (todas selecionadas inicialmente)
            # Com sidebar mostramos as colunas à esquerda da página, proporcionando uma interface mais organizada
            colunas_selecionadas = st.sidebar.multiselect(
                "Selecione as colunas para exibir:", colunas_disponiveis, default=colunas_disponiveis
            )

            # Exibir DataFrame com colunas selecionadas
            if colunas_selecionadas:
                st.write("### Dados do XML convertidos para DataFrame:")
                st.dataframe(df[colunas_selecionadas], hide_index=True)
            else:
                st.warning("Selecione pelo menos uma coluna para exibição.")

            # Resumo dos dados gerados
            # Aqui vamos apresentar métricas com somas, contagens e outros cálculos dos dados carregados.
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total BC ICMS", df['BC_ICMS'].sum().round(2))
            col2.metric("Total Vlr ICMS", df['vlr_ICMS'].sum().round(2))
            col3.metric("Total BC Pis", df['BC_PIS_Calc'].sum().round(2))
            col4.metric("Total BC Cofins", df['BC_Cofins_Calc'].sum().round(2))
           

            col1 = st.columns(1)[0]
            col1.metric("Contagem de documentos", df['Chave_Acesso'].nunique())

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

    with tab2:
        consulta_mva = df[['CNPJ_Destinatário','Numero_NF','Nome_Produto','NCM', 'CFOP', 'CEST', 'Origem_Produto', 'CST_ICMS', 'Vlr_Produto',
           'BC_ICMS', 'Per_ICMS', 'vlr_ICMS', 'Orig_Prod10', 'CST_ICMS10',
           'BC_ICMS10', 'Per_ICMS10', 'Vlr_ICMS10', 'Per_MVAST', 'BC_ICMS_ST', 'Per_ICMS_ST',
           'Vlr_ICMS_ST', 'Orig_Prod61', 'BC_ICMS61', 'Per_ICMS61', 'vlr_ICMS61']]
        tab2.write(consulta_mva[consulta_mva['CNPJ_Destinatário'].isin(['83299743001294']) & (consulta_mva['Per_MVAST'] > 0)].drop(['CNPJ_Destinatário'], axis=1)\
        .groupby(by=['Nome_Produto','CEST','Per_ICMS','Per_ICMS_ST','Per_MVAST'])[['Vlr_Produto','BC_ICMS', 'vlr_ICMS', 'BC_ICMS10','Vlr_ICMS10', 'BC_ICMS_ST', 'Vlr_ICMS_ST']].sum())

    with tab3:
        tab3.html(
            """
            <div style="text-align": center;">
                    <h3>Relatório de PisCofins Sintético por Produto</h3>
            </div>
            """)
        tab3.data_editor(duckdb.sql(
                        """
                        SELECT Numero_NF, 
                               Nome_Produto, 
                               NCM,BC_ICMS, 
                               Per_ICMS, 
                               vlr_ICMS, 
                               BC_ICMS61, 
                               Per_ICMS61, 
                               vlr_ICMS61,
                               Vlr_Produto,
                               Vlr_Desconto,
                               Vlr_Produto - Vlr_Desconto AS Vlr_Liquido,
                               BC_PIS_Calc, 
                               VLR_PIS_Calc, 
                               BC_Cofins_Calc, 
                               VLR_Cofins_Calc
                        FROM df
                        """).df(), hide_index=True)
        tab3.write(duckdb.sql("SELECT Nome_Produto, NCM FROM df").df())


    
