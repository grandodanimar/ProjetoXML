# Projeto para Extração de dados de Arquivos .xml
# Este projeto visa importar dados específicos constantes em arquivos.xml de notas fiscais eletrônicas modelo 55 para posterior importação em sistemas,
# bancos de dados e ferramentas de análise, com o intuito de facilitar a conferência dos respectivos dados no dia-a-dia e realização de cálculos de impostos.

import pandas as pd
import os
from io import StringIO
import xml.etree.ElementTree as ET

# Namespace do XML
ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

# Carrega dados do diretório
diretorio = 'arquivos_xml'

# Cria uma lista vazia para posteriormente converter em Dataframe para armazenar os dados
dataframes = []

# Iterar pelos arquivos no diretório
for arquivo in os.listdir(diretorio):
    if arquivo.endswith(".xml"):  # Filtra apenas arquivos XML
        caminho_completo = os.path.join(diretorio, arquivo)
        try:
            print(f"Processando: {arquivo}")
            tree = ET.parse(caminho_completo)
        except ET.ParseError as e:
            print(f"Erro ao processar {arquivo}: {e}")

        root = tree.getroot()

        # Inicializar variáveis gerais da nota fiscal
        chave_acesso = numero_nota = nat_op = data_emissao = None
        emitente_nome = cnpj_emitente = destinatario_nome = cnpj_destinatario = None

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

        # Capturar os dados do destinatário
        dest = root.find('.//nfe:infNFe/nfe:dest', ns)
        if dest is not None:
            destinatario_nome = dest.find('nfe:xNome', ns).text if dest.find('nfe:xNome', ns) is not None else None
            cnpj_destinatario = dest.find('nfe:CNPJ', ns).text if dest.find('nfe:CNPJ', ns) is not None else None

        # Capturar os dados de cada produto
        for det in root.findall('.//nfe:det', ns):
            produtos = det.find('nfe:prod', ns)
            if produtos is not None:
                cod_ean = produtos.find('nfe:cEANTrib', ns).text if produtos.find('nfe:cEANTrib', ns) is not None else None
                nome_produto = produtos.find('nfe:xProd', ns).text if produtos.find('nfe:xProd', ns) is not None else None
                ncm = produtos.find('nfe:NCM', ns).text if produtos.find('nfe:NCM', ns) is not None else None
                cest = produtos.find('nfe:CEST', ns).text if produtos.find('nfe:CEST', ns) is not None else None
                cfop = produtos.find('nfe:CFOP', ns).text if produtos.find('nfe:CFOP', ns) is not None else None
                vprod = produtos.find('nfe:vProd', ns).text if produtos.find('nfe:vProd', ns) is not None else None



                # Capturar os dados dos impostos (ICMS e PIS)
                imposto = det.find('nfe:imposto', ns)
                cst_icms = origem_prod = vBC_icms = pICMS = vICMS = None
                if imposto is not None:
                    icms00 = imposto.find('nfe:ICMS/nfe:ICMS00', ns)
                    if icms00 is not None:
                        origem_prod = icms00.find('nfe:orig', ns).text if icms00.find('nfe:orig', ns) is not None else None
                        cst_icms = icms00.find('nfe:CST', ns).text if icms00.find('nfe:CST', ns) is not None else None
                        vBC_icms = icms00.find('nfe:vBC', ns).text if icms00.find('nfe:vBC', ns) is not None else None
                        pICMS = icms00.find('nfe:pICMS', ns).text if icms00.find('nfe:pICMS', ns) is not None else None
                        vICMS = icms00.find('nfe:vICMS', ns).text if icms00.find('nfe:vICMS', ns) is not None else None
                
                cst_icms10 = origem_prod10 = vBC_icms10 = pICMS10 = vICMS10 = vBC_icms_ST = pICMS_ST = vICMS_ST = None
                
                if imposto is not None:
                    icms10 = imposto.find('nfe:ICMS/nfe:ICMS10', ns) if imposto is not None else None
                    if icms10 is not None:
                        origem_prod10 = icms10.find('nfe:orig', ns).text if icms10 is not None else None
                        cst_icms10 = icms10.find('nfe:CST', ns).text if icms10 is not None else None
                        vBC_icms10 = icms10.find('nfe:vBC', ns).text if icms10 is not None else None
                        pICMS10 = icms10.find('nfe:pICMS', ns).text if icms10 is not None else None
                        vICMS10 = icms10.find('nfe:vICMS', ns).text if icms10 is not None else None
                        vBC_icms_ST = icms10.find('nfe:vBC', ns).text if icms10 is not None else None
                        pICMS_ST = icms10.find('nfe:pICMS', ns).text if icms10 is not None else None
                        vICMS_ST = icms10.find('nfe:vICMS', ns).text if icms10 is not None else None
                
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
                cst_cofins = vBC_Cofins= pCofins = vCofins = cst_pis = vBC_pis = pPIS = vPIS = None
                
                if imposto is not None:
                    
                    pisaliq = imposto.find('nfe:PIS/nfe:PISAliq', ns) if imposto is not None else None
                    if pisaliq is not None:
                        cst_pis = pisaliq.find('nfe:CST', ns).text if pisaliq is not None else None
                        vBC_pis = pisaliq.find('nfe:vBC',ns).text if pisaliq is not None else None
                        pPIS = pisaliq.find('nfe:pPIS', ns).text if pisaliq is not None else None
                        vPIS = pisaliq.find('nfe:vPIS', ns).text if pisaliq is not None else None
                        pisaliq = imposto.find('nfe:PIS/nfe:PISAliq', ns) if imposto is not None else None

                    cofinsaliq = imposto.find('nfe:COFINS/nfe:COFINSAliq', ns) if imposto is not None else None
                    if cofinsaliq is not None:
                        cst_cofins = cofinsaliq.find('nfe:CST', ns).text if cofinsaliq is not None else None
                        vBC_cofins = cofinsaliq.find('nfe:vBC',ns).text if cofinsaliq is not None else None
                        pCOFINS = cofinsaliq.find('nfe:pCOFINS', ns).text if cofinsaliq is not None else None
                        vCOFINS = cofinsaliq.find('nfe:vCOFINS', ns).text if cofinsaliq is not None else None
                    
                    pisnt = imposto.find('nfe:PIS/nfe:PISNT', ns) if imposto is not None else None
                    if pisnt is not None:
                        cst_pis_nt = pisnt.find('nfe:CST', ns).text if pisnt is not None else None
                        vBC_pis_nt = pisnt.find('nfe:vBC',ns).text if pisnt is not None else None
                        pPIS_nt = pisnt.find('nfe:pPIS', ns).text if pisnt is not None else None
                        vPIS_nt = pisnt.find('nfe:vPIS', ns).text if pisnt is not None else None
                        
                    cofinsnt = imposto.find('nfe:COFINS/nfe:COFINSNT', ns) if imposto is not None else None
                    if cofinsnt is not None:
                        cst_cofins_nt = cofinsnt.find('nfe:CST', ns).text if cofinsnt is not None else None
                        vBC_cofins_nt = cofinsnt.find('nfe:vBC',ns).text if cofinsnt is not None else None
                        pCOFINS_nt = cofinsnt.find('nfe:pCOFINS', ns).text if cofinnt is not None else None
                        vCOFINS_nt = cofinsnt.find('nfe:vCOFINS', ns).text if cofinnt is not None else None
                        
                # Adicionar o produto como um registro ao dataframes
                dataframes.append({
                    'Nat_Operacao': nat_op,
                    'Chave_Acesso': chave_acesso,
                    'Numero_NF': numero_nota,
                    'Data_Emissao': data_emissao,
                    'CNPJ_Emitente': cnpj_emitente,
                    'Nome_Emitente': emitente_nome,
                    'CNPJ_Destinatário': cnpj_destinatario,
                    'Nome_Destinatário': destinatario_nome,
                    'Código_EAN': cod_ean,
                    'Nome_Produto': nome_produto,
                    'NCM': ncm,
                    'CFOP': cfop,
                    'CEST': cest,
                    'Origem_Produto': origem_prod,
                    'CST_ICMS': cst_icms,
                    'Vlr_Produto': vprod,
                    'BC_ICMS': vBC_icms,
                    'Per_ICMS': pICMS,
                    'vlr_ICMS': vICMS,
                    'Orig_Prod10': origem_prod10,
                    'CST_ICMS10': cst_icms10,
                    'BC_ICMS10': vBC_icms10,
                    'Per_ICMS10': pICMS10,
                    'vlr_ICMS10': vICMS10,
                    'BC_ICMS_ST': vBC_icms_ST,
                    'Per_ICMS_ST': pICMS_ST,
                    'Vlr_ICMS_ST': vICMS_ST,
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
                    'BC_Pis_nt': vBC_pis_nt,
                    'Per_Pis_nt': pPIS_nt,
                    'Vlr_Pis_nt': vPIS_nt,
                    'CST_COFINS_NT': cst_cofins_nt,
                    'BC_Cofins_nt': vBC_cofins_nt,
                    'Per_Cofins_nt': pCOFINS_nt,
                    'Vlr_Cofins_nt': vCOFINS_nt
                    
                })

# Criar um DataFrame com os dados de todos os arquivos
df = pd.DataFrame(dataframes)

# Converter colunas numéricas
cols_num_float = ['BC-ICMS','BC-ICMS61','vlr_ICMS61' ,'vlr_ICMS','BC-ICMS10', 'vlr_ICMS10', 'BC_ICMS-ST','Vlr_ICMS-ST','Vlr_Produto','BC_Pis', 'Per_Pis','Vlr_Pis','BC_Cofins', 'Per_Cofins', 'Vlr_Cofins','BC_Pis_nt', 'Per_Pis_nt','Vlr_Pis_nt','BC_Cofins_nt', 'Per_Cofins_nt', 'Vlr_Cofins_nt']
df[cols_num_float]= df[cols_num_float].apply(pd.to_numeric, errors='coerce', downcast='float')

cols_int = ['Per_ICMS','Per_ICMS10','Per_ICMS-ST', ]
df[cols_int] = df[cols_int].apply(pd.to_numeric, errors='coerce', downcast='integer')

# Converter coluna de data
df['Data_Emissao'] = pd.to_datetime(df['Data_Emissao'], utc=True).dt.strftime('%Y-%m-%d')

# Na conversão anterior a coluna continuará sendo string, portanto precisamos fazer a conversão novamente, porém sem dt.strftime()
df['Data_Emissao'] = pd.to_datetime(df['Data_Emissao'])

# o DataFrame apresenta valores nulos ou ausentes apenas nas colunas numéricas, desta forma preencheremos os mesmos com "0"
df = df.fillna(0)

# Inserindo novas colunas para Base de cálculo do ICMS e Valor do ICMS, concatenando as colunas com ICMS61 e ICMS
df['BC_ICMS'] = df['BC-ICMS'] + df['BC-ICMS61']
df['VLR_ICMS'] = df['vlr_ICMS'] + df['vlr_ICMS61']

# Calcular um valor na coluna BC_Pis e Vlr_Pis subtraindo o valor da coluna VLR_ICMS da coluna bc_icms
df['BC_PIS_Calc'] = df['Vlr_Produto'] - df['VLR_ICMS']
df['VLR_PIS_Calc'] = df['BC_PIS_Calc'] * 0.0165

df['BC_Cofins_Calc'] = df['Vlr_Produto'] - df['VLR_ICMS']
df['VLR_Cofins_Calc'] = df['BC_Cofins_Calc'] * 0.076

# Exportar para CSV
df.to_csv('notas_fiscais_fornecedores.csv', sep=';')