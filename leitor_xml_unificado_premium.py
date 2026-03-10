import streamlit as st
import pandas as pd
import numpy as np
import io
import duckdb
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO

# Configuração da página
st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded",
    page_title="Leitor XML Unificado (NFe & CTe)"
)

# Namespaces
ns_nfe = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
ns_cte = {'cte': 'http://www.portalfiscal.inf.br/cte'}

@st.cache_resource(scope='session')
def get_connection():
    """Cria e mantém a conexão com o DuckDB em memória."""
    con = duckdb.connect(database=':memory:')
    
    # Tabela NFe Completa (Script 1)
    con.execute("""
        CREATE TABLE IF NOT EXISTS notas_fiscais_xml (
            Nat_Operacao VARCHAR, Chave_Acesso VARCHAR, mod_doc INTEGER, serie INTEGER, numero INTEGER, 
            Data_Emissao DATE, CNPJ_Emitente VARCHAR, CPF_Emitente VARCHAR, Nome_Emitente VARCHAR, 
            UF_Emitente TEXT, CNPJ_Destinatário VARCHAR, Nome_Destinatário VARCHAR, UF_Destinatário TEXT, 
            Código_EAN VARCHAR, Nome_Produto VARCHAR, NCM TEXT, CFOP INTEGER, CEST TEXT, 
            cBenef TEXT, cCredPresumido TEXT, pCredPresumido FLOAT, vCredPresumido FLOAT, 
            Origem_Produto FLOAT, CST_ICMS VARCHAR, Cod_Prod VARCHAR, Qtde_Trib FLOAT, 
            Vlr_Unit FLOAT, Vlr_Produto FLOAT, Vlr_Desconto FLOAT, BC_ICMS FLOAT, 
            Per_ICMS FLOAT, vlr_ICMS FLOAT, CST_ICMS20 INTEGER, Orig_Prod20 INTEGER, 
            Per_RedBC FLOAT, BC_ICMS20 FLOAT, Per_ICMS20 FLOAT, Vlr_ICMS20 FLOAT, 
            Orig_Prod10 INTEGER, CST_ICMS10 INTEGER, BC_ICMS10 FLOAT, Per_ICMS10 FLOAT, 
            Vlr_ICMS10 FLOAT, Per_MVAST FLOAT, BC_ICMS_ST FLOAT, Per_ICMS_ST FLOAT, 
            Vlr_ICMS_ST FLOAT, BC_FCPST FLOAT, Per_FCPST FLOAT, Vlr_FCPST FLOAT, 
            Orig_Prod51 INTEGER, CST_ICMS51 INTEGER, BC_ICMS51 FLOAT, Per_ICMS51 FLOAT, 
            Per_Dif FLOAT, Vlr_ICMS_Op FLOAT, Vlr_ICMS_Dif FLOAT, Vlr_ICMS51 FLOAT, 
            Orig_Prod61 INTEGER, BC_ICMS61 FLOAT, Per_ICMS61 FLOAT, vlr_ICMS61 FLOAT, 
            Orig_Prod90 INTEGER, CST_ICMS90 INTEGER, OutrasDespAces FLOAT, CST_IPI INTEGER, 
            CST_IPI_NT INTEGER, BC_IPI FLOAT, Per_IPI FLOAT, Vlr_IPI FLOAT, CST_PIS VARCHAR, 
            BC_Pis FLOAT, Per_Pis FLOAT, Vlr_Pis FLOAT, CST_COFINS VARCHAR, BC_Cofins FLOAT, 
            Per_Cofins FLOAT, Vlr_Cofins FLOAT, CST_PIS_NT VARCHAR, CST_COFINS_NT VARCHAR,
            CST_Pis_Outras VARCHAR,  BC_Pis_Outras FLOAT, Per_Pis_Outras FLOAT, Vlr_Pis_Outras FLOAT,
            CST_Cofins_Outras VARCHAR, BC_Cofins_Outras FLOAT, Per_Cofins_Outras FLOAT, Vlr_Cofins_Outras FLOAT,
            cst_ibscbs VARCHAR, cClassTrib VARCHAR, vBC_IBSCBS FLOAT, pIBSUF FLOAT, 
            vIBSUF FLOAT, pIBSMun FLOAT, vIBSMun FLOAT, pCBS FLOAT, vCBS FLOAT, 
            pDifIBS FLOAT, vDifIBS FLOAT, vDevTrib FLOAT
        );
    """)
    
    # Tabela CTe Completa (Script 2)
    con.execute("""
        CREATE TABLE IF NOT EXISTS cte_xml (
            natop VARCHAR, cfop INTEGER, chave_acesso VARCHAR, serie VARCHAR,numero INTEGER, 
            mod_doc INTEGER, data_emissao DATE, cod_munic_ini INTEGER, munic_ini VARCHAR, 
            cod_munic_fim INTEGER, munic_fim VARCHAR, cod_mun_emit INTEGER, mun_emit VARCHAR, 
            emitente_nome VARCHAR, cnpj_emitente VARCHAR, uf_emitente VARCHAR, 
            destinatario_nome VARCHAR, cnpj_destinatario VARCHAR, uf_destinatario VARCHAR, 
            nome_remetente VARCHAR, cnpj_remetente VARCHAR, mun_remetente VARCHAR,
            cod_mun_remetente VARCHAR, vlr_tot_prest FLOAT, cst_icms VARCHAR, bc_icms FLOAT, per_icms FLOAT, 
            vlr_icms FLOAT, cst_icmsouf VARCHAR, bc_icmsouf FLOAT, per_icmsouf FLOAT, vlr_icmsouf FLOAT, 
            cst_ibscbs VARCHAR, clas_trib_ibscbs VARCHAR, bc_ibscbs FLOAT, 
            per_ibsuf FLOAT, vlr_ibsuf FLOAT, per_ibsmun FLOAT, vlr_ibsmun FLOAT, 
            per_cbs FLOAT, vlr_cbs FLOAT
        );
    """)
    return con

def get_text(parent, tag, ns):
    if parent is None: return None
    el = parent.find(tag, ns)
    return el.text if el is not None else None

def extrair_nfe(root, con):
    """Lógica completa de extração NFe (Script 1)."""
    chave_acesso = numero_nota = nat_op = data_emissao = mod_doc = None
    emitente_nome = cnpj_emitente = cpf_emitente = uf_emitente = destinatario_nome = cnpj_destinatario = uf_destinatario =vdesc=vprod= None

    protNFe = root.find('.//nfe:protNFe/nfe:infProt', ns_nfe)
    if protNFe is not None:
        chave_acesso = get_text(protNFe, 'nfe:chNFe', ns_nfe)

    ide = root.find('.//nfe:infNFe/nfe:ide', ns_nfe)
    if ide is not None:
        mod_doc = get_text(ide, 'nfe:mod', ns_nfe)
        serie = get_text(ide, 'nfe:serie', ns_nfe) or 0
        numero = get_text(ide, 'nfe:nNF', ns_nfe)
        nat_op = get_text(ide, 'nfe:natOp', ns_nfe)
        data_emissao = get_text(ide, 'nfe:dhEmi', ns_nfe)

    emit = root.find('.//nfe:infNFe/nfe:emit', ns_nfe)
    if emit is not None:
        emitente_nome = get_text(emit, 'nfe:xNome', ns_nfe)
        cnpj_emitente = get_text(emit, 'nfe:CNPJ', ns_nfe)
        cpf_emitente = get_text(emit, 'nfe:CPF', ns_nfe)
        enderEmit = emit.find('nfe:enderEmit', ns_nfe)
        if enderEmit is not None:
            uf_emitente = get_text(enderEmit, 'nfe:UF', ns_nfe)

    dest = root.find('.//nfe:infNFe/nfe:dest', ns_nfe)
    if dest is not None:
        destinatario_nome = get_text(dest, 'nfe:xNome', ns_nfe)
        cnpj_destinatario = get_text(dest, 'nfe:CNPJ', ns_nfe)
        enderDest = dest.find('nfe:enderDest', ns_nfe)
        if enderDest is not None:
            uf_destinatario = get_text(enderDest, 'nfe:UF', ns_nfe)

    for det in root.findall('.//nfe:det', ns_nfe):
        prod = det.find('nfe:prod', ns_nfe)
        if prod is not None:
            codProd = get_text(prod, 'nfe:cProd', ns_nfe)
            cod_ean = get_text(prod, 'nfe:cEANTrib', ns_nfe)
            nome_produto = get_text(prod, 'nfe:xProd', ns_nfe)
            ncm = get_text(prod, 'nfe:NCM', ns_nfe)
            cest = get_text(prod, 'nfe:CEST', ns_nfe)
            cfop = get_text(prod, 'nfe:CFOP', ns_nfe)
            qtdeTrib = get_text(prod, 'nfe:qTrib', ns_nfe)
            vlrUnit = get_text(prod, 'nfe:vUnTrib', ns_nfe)
            vprod = get_text(prod, 'nfe:vProd', ns_nfe)
            vdesc = get_text(prod, 'nfe:vDesc', ns_nfe)
            cbenef = get_text(prod, 'nfe:cBenef', ns_nfe)
            voutro = get_text(prod, 'nfe:vOutro', ns_nfe)

            gcred = prod.find('nfe:gCred', ns_nfe)
            cCredPresumido = pCredPresumido = vCredPresumido = None
            if gcred is not None:
                cCredPresumido = get_text(gcred, 'nfe:cCredPresumido', ns_nfe)
                pCredPresumido = get_text(gcred, 'nfe:pCredPresumido', ns_nfe)
                vCredPresumido = get_text(gcred, 'nfe:vCredPresumido', ns_nfe)

            imposto = det.find('nfe:imposto', ns_nfe)
            cst_icms = origem_prod = vBC_icms_prop = pICMS_prop = vICMS_prop = None
            cst_icms20 = origem_prod20 = per_redBC = vBC_icms20 = pICMS20 = vICMS20 = None
            cst_icms10 = origem_prod10 = pICMS_st = vBC_icms_st = vICMS_st = vBC_icms10 = pICMS10 = vICMS10 = pMVA_ST = None
            vBC_FCPST = pFCPST = vFCPST = None
            origem_prod61 = cst_icms61 = vBC_icms61 = pICMS61 = vICMS61 = None
            origem_prod51 = cst_icms51 = vBC_icms51 = pICMS51 = per_Dif = vICMSop = vICMSdif = vICMS51 = None
            orig_prod90 = cst_icms90 = None
            cst_pis = vBC_pis = pPIS = vPIS = cst_cofins = vBC_cofins = pCOFINS = vCOFINS = None
            cst_pis_nt = cst_cofins_nt =cst_pis_outr = cst_cofins_outr= cst_ipi = cst_ipi_nt = vBC_IPI = pIPI = vIPI = None
            cst_ibscbs = cclasstrib = vBC_IBSCBS = pIBSUF = vIBSUF = pIBSMun = vIBSMun = pCBS = vCBS = pDifIBS = vDifIBS = vDevTrib = None
            bc_pis_outr = vlr_pis_outr = per_pis_outr = bc_cofins_outr = vlr_cofins_outr = per_cofins_outr = None

            if imposto is not None:
                # ICMS 00
                icms00 = imposto.find('nfe:ICMS/nfe:ICMS00', ns_nfe)
                if icms00 is not None:
                    origem_prod = get_text(icms00, 'nfe:orig', ns_nfe)
                    cst_icms = get_text(icms00, 'nfe:CST', ns_nfe)
                    vBC_icms_prop = get_text(icms00, 'nfe:vBC', ns_nfe)
                    pICMS_prop = get_text(icms00, 'nfe:pICMS', ns_nfe)
                    vICMS_prop = get_text(icms00, 'nfe:vICMS', ns_nfe)
                # ICMS 20
                icms20 = imposto.find('nfe:ICMS/nfe:ICMS20', ns_nfe)
                if icms20 is not None:
                    origem_prod20 = get_text(icms20, 'nfe:orig', ns_nfe)
                    cst_icms20 = get_text(icms20, 'nfe:CST', ns_nfe)
                    per_redBC = get_text(icms20, 'nfe:pRedBC', ns_nfe)
                    vBC_icms20 = get_text(icms20, 'nfe:vBC', ns_nfe)
                    pICMS20 = get_text(icms20, 'nfe:pICMS', ns_nfe)
                    vICMS20 = get_text(icms20, 'nfe:vICMS', ns_nfe)
                # ICMS 10
                icms10 = imposto.find('nfe:ICMS/nfe:ICMS10', ns_nfe)
                if icms10 is not None:
                    origem_prod10 = get_text(icms10, 'nfe:orig', ns_nfe)
                    cst_icms10 = get_text(icms10, 'nfe:CST', ns_nfe)
                    vBC_icms_st = get_text(icms10, 'nfe:vBCST', ns_nfe)
                    vBC_icms10 = get_text(icms10, 'nfe:vBC', ns_nfe)
                    pICMS10 = get_text(icms10, 'nfe:pICMS', ns_nfe)
                    vICMS10 = get_text(icms10, 'nfe:vICMS', ns_nfe)
                    pICMS_st = get_text(icms10, 'nfe:pICMSST', ns_nfe)
                    vICMS_st = get_text(icms10, 'nfe:vICMSST', ns_nfe)
                    pMVA_ST = get_text(icms10, 'nfe:pMVAST', ns_nfe)
                    vBC_FCPST = get_text(icms10, 'nfe:vBCFCPST', ns_nfe)
                    pFCPST = get_text(icms10, 'nfe:pFCPST', ns_nfe)
                    vFCPST = get_text(icms10, 'nfe:vFCPST', ns_nfe)
                # ICMS 61
                icms61 = imposto.find('nfe:ICMS/nfe:ICMS61', ns_nfe)
                if icms61 is not None:
                    origem_prod61 = get_text(icms61, 'nfe:orig', ns_nfe)
                    cst_icms61 = get_text(icms61, 'nfe:CST', ns_nfe)
                    vBC_icms61 = get_text(icms61, 'nfe:qBCMonoRet', ns_nfe)
                    pICMS61 = get_text(icms61, 'nfe:adRemICMSRet', ns_nfe)
                    vICMS61 = get_text(icms61, 'nfe:vICMSMonoRet', ns_nfe)
                # ICMS 51
                icms51 = imposto.find('nfe:ICMS/nfe:ICMS51', ns_nfe)
                if icms51 is not None:
                    origem_prod51 = get_text(icms51, 'nfe:orig', ns_nfe)
                    cst_icms51 = get_text(icms51, 'nfe:CST', ns_nfe)
                    vBC_icms51 = get_text(icms51, 'nfe:vBC', ns_nfe)
                    pICMS51 = get_text(icms51, 'nfe:pICMS', ns_nfe)
                    per_Dif = get_text(icms51, 'nfe:pDif', ns_nfe)
                    vICMSop = get_text(icms51, 'nfe:vICMSOp', ns_nfe)
                    vICMSdif = get_text(icms51, 'nfe:vICMSDif', ns_nfe)
                    vICMS51 = get_text(icms51, 'nfe:vICMS', ns_nfe)
                # ICMS 90
                icms90 = imposto.find('nfe:ICMS/nfe:ICMS90', ns_nfe)
                if icms90 is not None:
                    orig_prod90 = get_text(icms90, 'nfe:orig', ns_nfe)
                    cst_icms90 = get_text(icms90, 'nfe:CST', ns_nfe)
                # PIS/COFINS/IPI
                pisaliq = imposto.find('nfe:PIS/nfe:PISAliq', ns_nfe)
                if pisaliq is not None:
                    cst_pis = get_text(pisaliq, 'nfe:CST', ns_nfe)
                    vBC_pis = get_text(pisaliq, 'nfe:vBC', ns_nfe)
                    pPIS = get_text(pisaliq, 'nfe:pPIS', ns_nfe)
                    vPIS = get_text(pisaliq, 'nfe:vPIS', ns_nfe)
                cofinsaliq = imposto.find('nfe:COFINS/nfe:COFINSAliq', ns_nfe)
                if cofinsaliq is not None:
                    cst_cofins = get_text(cofinsaliq, 'nfe:CST', ns_nfe)
                    vBC_cofins = get_text(cofinsaliq, 'nfe:vBC', ns_nfe)
                    pCOFINS = get_text(cofinsaliq, 'nfe:pCOFINS', ns_nfe)
                    vCOFINS = get_text(cofinsaliq, 'nfe:vCOFINS', ns_nfe)
                
                pisnt = imposto.find('nfe:PIS/nfe:PISNT', ns_nfe)
                if pisnt is not None: 
                    cst_pis_nt = get_text(pisnt, 'nfe:CST', ns_nfe)

                cofinsnt = imposto.find('nfe:COFINS/nfe:COFINSNT', ns_nfe)
                if cofinsnt is not None: 
                    cst_cofins_nt = get_text(cofinsnt, 'nfe:CST', ns_nfe)

                pisoutr = imposto.find('nfe:PIS/nfe:PISOutr', ns_nfe)
                if pisoutr is not None:
                    cst_pis_outr = get_text(pisoutr,'nfe:CST', ns_nfe)
                    bc_pis_outr = get_text(pisoutr, 'nfe:vBC', ns_nfe)
                    per_pis_outr = get_text(pisoutr, 'nfe:pPIS', ns_nfe)
                    vlr_pis_outr = get_text(pisoutr, 'nfe:vPIS', ns_nfe)

                cofinsoutr = imposto.find('nfe:COFINS/nfe:COFINSOutr', ns_nfe)
                if cofinsoutr is not None:
                    cst_cofins_outr = get_text(pisoutr, 'nfe:CST', ns_nfe)
                    bc_cofins_outr = get_text(pisoutr, 'nfe:vBC', ns_nfe)
                    per_cofins_outr = get_text(pisoutr, 'nfe:pCOFINS', ns_nfe)
                    vlr_cofins_outr = get_text(pisoutr, 'nfe:vCOFINS', ns_nfe)

                ipint = imposto.find('nfe:IPI/nfe:IPINT', ns_nfe)
                if ipint is not None: 
                    cst_ipi_nt = get_text(ipint, 'nfe:CST', ns_nfe)

                ipitrib = imposto.find('nfe:IPI/nfe:IPITrib', ns_nfe)
                if ipitrib is not None:
                    cst_ipi = get_text(ipitrib, 'nfe:CST', ns_nfe)
                    vBC_IPI = get_text(ipitrib, 'nfe:vBC', ns_nfe)
                    pIPI = get_text(ipitrib, 'nfe:pIPI', ns_nfe)
                    vIPI = get_text(ipitrib, 'nfe:vIPI', ns_nfe)
                # IBS/CBS
                ibscbs = imposto.find('nfe:IBSCBS', ns_nfe)
                if ibscbs is not None:
                    cst_ibscbs = get_text(ibscbs, 'nfe:CST', ns_nfe)
                    cclasstrib = get_text(ibscbs, 'nfe:cClassTrib', ns_nfe)
                    gIBSCBS = ibscbs.find('nfe:gIBSCBS', ns_nfe)
                    if gIBSCBS is not None:
                        vBC_IBSCBS = get_text(gIBSCBS, 'nfe:vBC', ns_nfe)
                        gIBSUF = gIBSCBS.find('nfe:gIBSUF', ns_nfe)
                        if gIBSUF is not None:
                            pIBSUF = get_text(gIBSUF, 'nfe:pIBSUF', ns_nfe)
                            vIBSUF = get_text(gIBSUF, 'nfe:vIBSUF', ns_nfe)
                        gDif = gIBSCBS.find('nfe:gDif', ns_nfe)
                        if gDif is not None:
                            pDifIBS = get_text(gDif, 'nfe:pDif', ns_nfe)
                            vDifIBS = get_text(gDif, 'nfe:vDif', ns_nfe)
                        gDevTrib = gIBSCBS.find('nfe:gDevTrib', ns_nfe)
                        if gDevTrib is not None:
                            vDevTrib = get_text(gDevTrib, 'nfe:vDevTrib', ns_nfe)
                        gIBSMun = gIBSCBS.find('nfe:gIBSMun', ns_nfe)
                        if gIBSMun is not None:
                            pIBSMun = get_text(gIBSMun, 'nfe:pIBSMun', ns_nfe)
                            vIBSMun = get_text(gIBSMun, 'nfe:vIBSMun', ns_nfe)
                        gCBS = gIBSCBS.find('nfe:gCBS', ns_nfe)
                        if gCBS is not None:
                            pCBS = get_text(gCBS, 'nfe:pCBS', ns_nfe)
                            vCBS = get_text(gCBS, 'nfe:vCBS', ns_nfe)

            #bc_pis_calc = vprod - vICMS_prop - df['Vlr_Desconto']

            con.execute("""
                INSERT INTO notas_fiscais_xml VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                )
            """, (nat_op, chave_acesso, mod_doc,serie, numero, data_emissao, cnpj_emitente, cpf_emitente, emitente_nome, uf_emitente, cnpj_destinatario, destinatario_nome,
                  uf_destinatario, cod_ean, nome_produto, ncm, cfop, cest, cbenef, cCredPresumido, pCredPresumido, vCredPresumido, origem_prod, cst_icms, codProd,
                  qtdeTrib, vlrUnit, vprod, vdesc, vBC_icms_prop, pICMS_prop, vICMS_prop, cst_icms20, origem_prod20, per_redBC, vBC_icms20, pICMS20, vICMS20, origem_prod10,
                  cst_icms10, vBC_icms10, pICMS10, vICMS10, pMVA_ST, vBC_icms_st, pICMS_st, vICMS_st, vBC_FCPST, pFCPST, vFCPST, origem_prod51, cst_icms51, vBC_icms51, pICMS51, per_Dif, vICMSop,
                  vICMSdif, vICMS51, origem_prod61, vBC_icms61, pICMS61, vICMS61, orig_prod90, cst_icms90, voutro, cst_ipi, cst_ipi_nt, vBC_IPI, pIPI, vIPI, cst_pis, vBC_pis, pPIS, vPIS, cst_cofins,
                  vBC_cofins, pCOFINS, vCOFINS, cst_pis_nt, cst_cofins_nt, cst_pis_outr,bc_pis_outr,per_pis_outr,vlr_pis_outr,cst_cofins_outr,bc_cofins_outr,per_cofins_outr, vlr_cofins_outr,
                  cst_ibscbs, cclasstrib, vBC_IBSCBS, pIBSUF, vIBSUF, pIBSMun, vIBSMun, pCBS, vCBS, pDifIBS, vDifIBS, vDevTrib))
    df = con.execute("select * from notas_fiscais_xml").df()

    # Converter colunas numéricas
    cols_num_float = ['BC_ICMS','BC_ICMS61','vlr_ICMS61' , 'Per_ICMS61','vlr_ICMS','BC_ICMS10', 'Vlr_ICMS10', 'Vlr_Produto','BC_Pis', 'Per_Pis','Vlr_Pis','BC_Cofins',
                  'Per_Cofins', 'Vlr_Cofins', 'Per_MVAST','Vlr_Unit', 'Qtde_Trib', 'Vlr_Desconto', 'BC_ICMS51', 'Per_ICMS51', 'Per_Dif', 'Vlr_ICMS_Op', 'Vlr_ICMS_Dif','Vlr_ICMS51', 
                     'Per_RedBC', 'BC_ICMS20', 'Per_ICMS20', 'Vlr_ICMS20', 'Per_ICMS_ST', 'BC_ICMS_ST', 'Vlr_ICMS_ST','BC_FCPST', 'Per_FCPST','Vlr_FCPST','vCredPresumido', 'pCredPresumido', 
                     'OutrasDespAces', 'vBC_IBSCBS', 'pIBSUF','vIBSUF', 'pIBSMun','vIBSMun','pCBS', 'vCBS', 'pDifIBS', 'vDifIBS', 'vDevTrib']
    df[cols_num_float]= df[cols_num_float].apply(lambda col: pd.to_numeric(col, errors='coerce').round(2))

    cols_int = ['Per_ICMS','Per_ICMS10']
    df[cols_int] = df[cols_int].apply(pd.to_numeric, errors='coerce', downcast='integer')

    # Converter coluna de data
    df['Data_Emissao'] = pd.to_datetime(df['Data_Emissao'], utc=True).dt.strftime('%Y-%m-%d')

    # Na conversão anterior a coluna continuará sendo string, portanto precisamos fazer a conversão novamente, porém sem dt.strftime()
    df['Data_Emissao'] = pd.to_datetime(df['Data_Emissao'], utc=True).dt.tz_convert(None)

    df = df[df['Data_Emissao'].notna()]

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


def extrair_cte(root, con):
    """Lógica completa de extração CTe (Script 2)."""
    chave_acesso = None
    protCTe = root.find('.//cte:protCTe/cte:infProt', ns_cte)
    if protCTe is not None:
        chave_acesso = get_text(protCTe, 'cte:chCTe', ns_cte)

    ide = root.find('.//cte:infCte/cte:ide', ns_cte)
    natop = cfop = numero = mod_doc = data_emissao = cod_munic_ini = munic_ini = cod_munic_fim = munic_fim = None
    if ide is not None:
        natop = get_text(ide, 'cte:natOp', ns_cte)
        cfop = get_text(ide, 'cte:CFOP', ns_cte)
        serie = get_text(ide,'cte:serie', ns_cte)
        numero = get_text(ide, 'cte:nCT', ns_cte)
        mod_doc = get_text(ide, 'cte:mod', ns_cte)
        data_emissao = get_text(ide, 'cte:dhEmi', ns_cte)
        cod_munic_ini = get_text(ide, 'cte:cMunIni', ns_cte)
        munic_ini = get_text(ide, 'cte:xMunIni', ns_cte)
        cod_munic_fim = get_text(ide, 'cte:cMunFim', ns_cte)
        munic_fim = get_text(ide, 'cte:xMunFim', ns_cte)

    emit = root.find('.//cte:infCte/cte:emit', ns_cte)
    emitente_nome = cnpj_emitente = uf_emitente = cod_mun_emit = mun_emit = None
    if emit is not None:
        emitente_nome = get_text(emit, 'cte:xNome', ns_cte)
        cnpj_emitente = get_text(emit, 'cte:CNPJ', ns_cte)
        enderEmit = emit.find('cte:enderEmit', ns_cte)
        if enderEmit is not None:
            uf_emitente = get_text(enderEmit, 'cte:UF', ns_cte)
            cod_mun_emit = get_text(enderEmit, 'cte:cMun', ns_cte)
            mun_emit = get_text(enderEmit, 'cte:xMun', ns_cte)

    rem = root.find('.//cte:infCte/cte:rem', ns_cte)
    nome_remetente=cnpj_remetente=uf_remetente=cod_mun_rem=mun_rem=None
    if rem is not None:
        nome_remetente = get_text(rem,'cte:xNome', ns_cte)
        cnpj_remetente = get_text(rem,'cte:CNPJ', ns_cte)
        uf_remetente = get_text(rem,'cte:UF', ns_cte)
        cod_mun_rem = get_text(rem,'cte:cMun', ns_cte)
        mun_rem = get_text(rem,'cte:xMun', ns_cte)


    dest = root.find('.//cte:infCte/cte:dest', ns_cte)
    destinatario_nome = cnpj_destinatario = uf_destinatario = None
    if dest is not None:
        destinatario_nome = get_text(dest, 'cte:xNome', ns_cte)
        cnpj_destinatario = get_text(dest, 'cte:CNPJ', ns_cte)
        enderDest = dest.find('cte:enderDest', ns_cte)
        if enderDest is not None:
            uf_destinatario = get_text(enderDest, 'cte:UF', ns_cte)

    vprest = root.find('.//cte:infCte/cte:vPrest', ns_cte)
    vlr_tot_prest = get_text(vprest, 'cte:vTPrest', ns_cte) if vprest is not None else 0

    cst_icms = bc_icms = per_icms = vlr_icms = cst_ibscbs = clas_trib_ibscbs = bc_ibscbs = per_ibsuf = vlr_ibsuf = per_ibsmun = vlr_ibsmun = per_cbs = vlr_cbs = None
    cst_icmsouf = bc_icmsouf = per_icmsouf = vlr_icmsouf = None
    imp = root.find('.//cte:infCte/cte:imp', ns_cte)
    if imp is not None:
        icms00 = imp.find('cte:ICMS/cte:ICMS00', ns_cte)
        if icms00 is not None:
            cst_icms = get_text(icms00, 'cte:CST', ns_cte)
            bc_icms = get_text(icms00, 'cte:vBC', ns_cte)
            per_icms = get_text(icms00, 'cte:pICMS', ns_cte)
            vlr_icms = get_text(icms00, 'cte:vICMS', ns_cte)
        icmsoutrauf = imp.find('cte:ICMS/cte:ICMSOutraUF', ns_cte)
        if icmsoutrauf is not None:
            cst_icmsouf = get_text(icmsoutrauf, 'cte:CST', ns_cte)
            bc_icmsouf = get_text(icmsoutrauf, 'cte:vBCOutraUF', ns_cte)
            per_icmsouf = get_text(icmsoutrauf, 'cte:pICMSOutraUF', ns_cte)
            vlr_icmsouf = get_text(icmsoutrauf, 'cte:vICMSOutraUF', ns_cte)

        ibscbs = imp.find('cte:IBSCBS', ns_cte)
        if ibscbs is not None:
            cst_ibscbs = get_text(ibscbs, 'cte:CST', ns_cte)
            clas_trib_ibscbs = get_text(ibscbs, 'cte:cClassTrib', ns_cte)
            gIBSCBS = ibscbs.find('cte:gIBSCBS', ns_cte)
            if gIBSCBS is not None:
                bc_ibscbs = get_text(gIBSCBS, 'cte:vBC', ns_cte)
                gIBSUF = gIBSCBS.find('cte:gIBSUF', ns_cte)
                if gIBSUF is not None:
                    per_ibsuf = get_text(gIBSUF, 'cte:pIBSUF', ns_cte)
                    vlr_ibsuf = get_text(gIBSUF, 'cte:vIBSUF', ns_cte)
                gIBSMun = gIBSCBS.find('cte:gIBSMun', ns_cte)
                if gIBSMun is not None:
                    per_ibsmun = get_text(gIBSMun, 'cte:pIBSMun', ns_cte)
                    vlr_ibsmun = get_text(gIBSMun, 'cte:vIBSMun', ns_cte)
                gCBS = gIBSCBS.find('cte:gCBS', ns_cte)
                if gCBS is not None:
                    per_cbs = get_text(gCBS, 'cte:pCBS', ns_cte)
                    vlr_cbs = get_text(gCBS, 'cte:vCBS', ns_cte)

    con.execute("""
        INSERT INTO cte_xml VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (natop, cfop, chave_acesso, serie, numero, mod_doc, data_emissao, cod_munic_ini, munic_ini, cod_munic_fim, munic_fim, cod_mun_emit, mun_emit, emitente_nome, cnpj_emitente, uf_emitente, 
          destinatario_nome, cnpj_destinatario, uf_destinatario, nome_remetente, cnpj_remetente, mun_rem, cod_mun_rem,vlr_tot_prest, cst_icms, bc_icms, per_icms, vlr_icms,cst_icmsouf, bc_icmsouf, per_icmsouf, vlr_icmsouf, 
          cst_ibscbs, clas_trib_ibscbs, bc_ibscbs, per_ibsuf, vlr_ibsuf, per_ibsmun, vlr_ibsmun, per_cbs, vlr_cbs))

    df_cte = con.execute("select * from cte_xml").df()

    return df_cte

def main():
    st.title("📊 Hub de Documentos Fiscais: NFe & CTe")
    
    con = get_connection()
    
    # Sidebar para Upload e Filtros
    with st.sidebar:
        st.header("Configurações")
        uploaded_files = st.file_uploader("Upload de arquivos XML", type="xml", accept_multiple_files=True)
        
        if uploaded_files:
            if st.button("Processar Arquivos"):
                # Limpar para novo processamento
                con.execute("DELETE FROM notas_fiscais_xml")
                con.execute("DELETE FROM cte_xml")
                for arquivo in uploaded_files:
                    try:
                        content = arquivo.read()
                        tree = ET.parse(io.BytesIO(content))
                        root = tree.getroot()
                        if root.tag.endswith('nfeProc') or root.find('.//nfe:infNFe', ns_nfe) is not None:
                            extrair_nfe(root, con)
                        elif root.tag.endswith('cteProc') or root.find('.//cte:infCte', ns_cte) is not None:
                            extrair_cte(root, con)
                    except Exception as e:
                        st.error(f"Erro no arquivo {arquivo.name}: {e}")
                st.success("Processamento concluído!")

    # Verificar se há dados
    df_nfe_raw = con.execute("SELECT * FROM notas_fiscais_xml").df()
    df_cte_raw = con.execute("SELECT * FROM cte_xml").df()

    if not df_nfe_raw.empty or not df_cte_raw.empty:
        # Preparar Tabela Unificada
        con.execute("""
            CREATE OR REPLACE TABLE documentos_unificados AS
            SELECT 
                'NFe' as Tipo_Doc,mod_doc,Serie, Chave_Acesso, numero, Data_Emissao, 
                CNPJ_Emitente, Nome_Emitente, CNPJ_Destinatário as CNPJ_Destinatario, 
                Nome_Destinatário as Nome_Destinatario, Vlr_Produto as Valor_Total,nome_produto, CFOP, NCM, UF_Emitente, vlr_produto, vlr_desconto, BC_ICMS10 as bc_icms10, Per_ICMS10 as per_icms10, Vlr_ICMS10 as vlr_icms10,
                BC_ICMS,BC_ICMS51,Per_ICMS51,BC_ICMS20, Per_ICMS20, Vlr_ICMS20,Per_ICMS as per_icms,vlr_icms, BC_ICMS_ST,Vlr_ICMS_ST,BC_FCPST,Vlr_FCPST,Vlr_IPI, Vlr_ICMS_Dif as Vlr_ICMS_Diferido, 
                Vlr_ICMS51, Vlr_ICMS_Op as Vlr_ICMS_Operacao,0.0 as cst_icmsouf, 0.0 as bc_icmsouf, 0.0 as per_icmsouf, 0.0 as vlr_icmsouf,OutrasDespAces,orig_prod61, BC_ICMS61, Per_ICMS61, vlr_ICMS61, BC_Pis, Per_Pis,Vlr_Pis,
                BC_Cofins, Per_Cofins, Vlr_Cofins, COALESCE(CAST(CST_PIS AS VARCHAR), CAST(CST_PIS_NT AS VARCHAR),CAST(CST_Pis_Outras AS VARCHAR)) as CST_PIS_UNIFICADO,
                cst_ibscbs, cclasstrib as clas_trib_ibscbs, vBC_IBSCBS as bc_ibscbs, pIBSUF as per_ibsuf, vIBSUF as vlr_ibsuf, pIBSMun as per_ibsmun, vIBSMun as vlr_ibsmun, 
                pCBS as per_cbs, vCBS as vlr_cbs, pDifIBS, vDifIBS, vDevTrib, Nome_Produto
                FROM notas_fiscais_xml
            UNION ALL
            SELECT 
                'CTe' as Tipo_Doc, mod_doc, Serie, chave_acesso as Chave_Acesso, numero, 
                data_emissao as Data_Emissao, cnpj_emitente as CNPJ_Emitente, 
                emitente_nome as Nome_Emitente, cnpj_destinatario as CNPJ_Destinatario, 
                destinatario_nome as Nome_Destinatario, vlr_tot_prest as Valor_Total,'Sem nome' as nome_produto, cfop as CFOP, 0 as NCM,uf_emitente as UF_Emitente,0.0 as vlr_produto, 0.0 as vlr_desconto, 0.0 as bc_icms10, 0.0 as per_icms10, 0.0 as vlr_icms10,
                bc_icms,0.0 AS BC_ICMS51,0.0 AS Per_ICMS51,0.0 AS BC_ICMS20 , 0.0 AS Per_ICMS20 , 0.0 AS Vlr_ICMS20,per_icms,vlr_icms,0.0 AS BC_ICMS_ST,0.0 as Vlr_ICMS_ST, 0.0 as BC_FCPST, 
                0.0 as Vlr_FCPST, 0.0 as Vlr_IPI,0.0 as Vlr_ICMS_Diferido,0.0 Vlr_ICMS51, 0.0 Vlr_ICMS_Operacao,cst_icmsouf, bc_icmsouf, per_icmsouf, vlr_icmsouf,0.0 as OutrasDespAces,
                '00' as orig_prod61, 0.0 as BC_ICMS61, 0.0 as Per_ICMS61, 0.0 as Vlr_ICMS61,
                0.0 as BC_Pis, 0.0 as Per_Pis, 0.0 as Vlr_Pis, 0.0 as BC_Cofins, 0.0 as Per_Cofins, 0.0 as Vlr_Cofins, '00' as CST_PIS_UNIFICADO,
                cst_ibscbs, clas_trib_ibscbs, bc_ibscbs, per_ibsuf, vlr_ibsuf, per_ibsmun, vlr_ibsmun, per_cbs, vlr_cbs, 0.0 as vDifIBS, 0.0 as vDifIBS, 0.0 as vDevTrib, 0 as Nome_Produto
            FROM cte_xml
        """)
        df_uni = con.execute("SELECT * FROM documentos_unificados").df()

        # Filtros Globais no Sidebar (após processamento)
        with st.sidebar:
            st.divider()
            st.subheader("Filtros Globais")
             # Encontra a data mínima e máxima no DataFrame para definir os limites do filtro
            min_date = df_uni["Data_Emissao"].min().date()
            max_date = df_uni["Data_Emissao"].max().date()
            
            # Cria o widget de seleção de intervalo de datas (calendário)
            # O valor padrão 'value' é uma tupla com o intervalo completo (min_date, max_date)
            date_range = st.sidebar.date_input("Datas de Emissão", (min_date, max_date), min_value = min_date, max_value = max_date)

            # Validação para garantir que o 'date_range' retornou um início e fim
            if len(date_range) == 2:
                start_date, end_date = date_range
            else:
                # Fallback caso algo dê errado (ex: usuário limpa o campo)
                start_date, end_date = min_date, max_date

            # Entrada do usuário
            nr_nf_filtro = st.text_input('Digite aqui um ou números de NF separados por vírgula', width=350)
            st.write("Ou")
            ch_acesso = st.text_input('Digite aqui uma ou mais Chaves de Acesso separadas por vírgula', width=500)

            # Lista com as entradas informadas
            numeros_nf = [nf.strip() for nf in nr_nf_filtro.split(',') if nf.strip()]
            ch_acesso_nf = [ch.strip() for ch in ch_acesso.split(',') if ch.strip()]

            cnpj_dest = df_uni['CNPJ_Destinatario'].unique().tolist()
            cnpj_selecionado = st.multiselect("Filtrar por CNPJ do Destinatário", cnpj_dest, default=cnpj_dest)

            # Filtro de Cfop
            all_CFOP = sorted(df_uni["CFOP"].unique())
            selected_CFOP = st.sidebar.multiselect("CFOP", all_CFOP, default = all_CFOP)

            all_cst_piscofins = df_uni['CST_PIS_UNIFICADO'].unique()
            selected_cstpis = st.sidebar.multiselect("CST PIS", all_cst_piscofins, default=all_cst_piscofins)
            
            # Aplicar filtros
            df_uni_filtrado = df_uni[

                # 1. Filtro de Data: Compara a data da linha com o 'start_date' e 'end_date'
                (df_uni['Data_Emissao'].dt.date >= start_date) &
                (df_uni['Data_Emissao'].dt.date <= end_date) &

                # 2.Filtro de números de documentos
                (df_uni["numero"].astype(str).isin(numeros_nf) if numeros_nf else True) &

                # 3.Filtro de CNPJ da empresa, em princípio vamos tratar como sendo apenas o do destinatário
                (df_uni["CNPJ_Destinatario"].isin(cnpj_selecionado)) &

                (df_uni['CFOP'].isin(selected_CFOP) &
                (df_uni['CST_PIS_UNIFICADO'].isin(selected_cstpis))
            )
            ].copy()
            
            # Criar view filtrada no DuckDB para os relatórios
            con.register('df_filtrado_uni', df_uni_filtrado)
            
        # Layout de Abas
        tab_resumo, tab_nfe, tab_cte, tab_reg_ent, tab_piscofins, tab_ref_trib = st.tabs(["📈 Resumo Unificado", "📄 NFes Detalhado", "🚛 CTes Detalhado", 
                                                                                            "📋 Registro de Entradas", "🏛 Resumo Pis e Coinf CST-CFOP", "Relatório IBS-CBS"])
        
        with tab_resumo:
            st.subheader("Visão Geral dos Documentos")
            c1, c2, c3 = st.columns(3)
            contagem_por_tipo = df_uni_filtrado.groupby('Tipo_Doc')['Chave_Acesso'].nunique()
            c1.metric("Total Documentos", value=df_uni_filtrado['Chave_Acesso'].nunique(), border=True)
            c2.metric("Total NFe", value=contagem_por_tipo.get('NFe',0), border=True)
            c3.metric("Total CTe", value=contagem_por_tipo.get('CTe',0), border=True)
            
            st.dataframe(df_uni_filtrado, use_container_width=True, hide_index=True)
            
            # Download
            csv = df_uni_filtrado.to_csv(index=False).encode('utf-8')
            st.download_button("Baixar Unificado (CSV)", csv, "documentos_unificados.csv", "text/csv")

        with tab_nfe:
            colunas_disponiveis = df_uni_filtrado.columns.tolist()

            # Seleção de colunas com valores padrão (todas selecionadas inicialmente)
            # Com sidebar mostramos as colunas à esquerda da página, proporcionando uma interface mais organizada
            colunas_selecionadas = st.multiselect(
                "Selecione as colunas para exibir:", colunas_disponiveis, default=colunas_disponiveis
            )

            # Exibir DataFrame com colunas selecionadas
            if colunas_selecionadas:
                st.write("### Dados do XML convertidos para Tabela:")
                st.dataframe(df_uni_filtrado[colunas_selecionadas], hide_index=True)
            else:
                st.warning("Selecione pelo menos uma coluna para exibição.")

        with tab_cte:
            st.subheader("Detalhamento Completo CTe")
            if not df_cte_raw.empty:
                st.dataframe(df_cte_raw, use_container_width=True)
            else:
                st.info("Nenhum CTe processado.")

        with tab_reg_ent:
            
            if not df_nfe_raw.empty:
                # Exemplo de Relatório de Registro de Entradas (Script 1)
                query_entradas = """
                    SELECT
                        Tipo_Doc,
                        Serie, 
                        Numero, 
                        Data_Emissao, 
                        UF_Emitente,
                        ROUND(SUM(
                            COALESCE(Valor_Total, 0) + 
                            COALESCE(Vlr_ICMS_ST, 0) + 
                            COALESCE(Vlr_FCPST, 0) + 
                            COALESCE(Vlr_IPI, 0) + 
                            COALESCE(OutrasDespAces, 0)
                        ), 2) AS Vlr_Contabil,
                        CASE
                            WHEN CFOP IN (6101,6102,6105) THEN 2102
                            WHEN CFOP IN (5101,5102,5105) THEN 1102
                            WHEN CFOP IN (5401,5403) THEN 1403
                            WHEN CFOP IN (5151) THEN 1151
                            WHEN CFOP IN (5152) THEN 1152
                            WHEN CFOP IN (6151) THEN 2151
                            WHEN CFOP IN (6152) THEN 2152
                            WHEN CFOP IN (5352) THEN 1352
                            WHEN CFOP IN (6352) THEN 2352
                            WHEN CFOP IN (5353) THEN 1353
                            WHEN CFOP IN (6353) THEN 2353
                            WHEN CFOP IN (5932) THEN 1932
                            WHEN CFOP IN (6932) THEN 2932
                            WHEN CFOP IN (5910) THEN 1910
                            WHEN CFOP IN (6910) THEN 2910
                            WHEN CFOP IN (6403,6401) THEN 2403
                            WHEN CFOP IN (6920) THEN 2920
                            WHEN CFOP IN (6949) THEN 2949
                            WHEN CFOP IN (5920) THEN 1920
                            WHEN CFOP IN (5949) THEN 1949
                        END AS CFOP_ENTRADA,
                        ROUND(SUM(COALESCE(BC_ICMS, 0)+
                                COALESCE(BC_ICMS10,0)+
                                COALESCE(BC_ICMS51,0)+
                                COALESCE(BC_ICMS20,0)+
                                COALESCE(bc_icmsouf,0)), 2) AS BC_ICMS,
                        ROUND(COALESCE(Per_ICMS,0) + 
                            COALESCE(Per_ICMS51,0)+
                            COALESCE(Per_ICMS20,0)+
                            COALESCE(Per_ICMS10,0)+
                            COALESCE(per_icmsouf,0),2) as Per_ICMS,
                        ROUND(SUM(COALESCE(vlr_ICMS, 0)+
                            COALESCE(Vlr_ICMS51,0)+
                            COALESCE(Vlr_ICMS20,0)+
                            COALESCE(Vlr_ICMS10,0)+
                            COALESCE(vlr_icmsouf,0)), 2) AS Vlr_ICMS,
                        ROUND(SUM(COALESCE(BC_ICMS_ST,0)),2) AS BC_ICMS_ST,
                        ROUND(SUM(Vlr_ICMS_ST),2) AS Vlr_ICMS_ST,
                        ROUND(SUM(Vlr_FCPST),2) AS Vlr_FCPST,
                        ROUND(SUM(Vlr_IPI),2) AS Vlr_IPI,
                        ROUND(SUM(Vlr_ICMS_Operacao),2) AS Vlr_ICMS_Operacao,
                        ROUND(SUM(Vlr_ICMS_Diferido),2) AS Vlr_ICMS_Diferido,
                        ROUND(SUM(Vlr_ICMS51),2) AS Vlr_ICMS51,
                        ROUND(SUM(OutrasDespAces),2) AS Desp_Acessorias,
                        ROUND(SUM(COALESCE(Vlr_IPI,0)),2) AS Vlr_IPI
                    FROM df_uni_filtrado
                    GROUP BY Serie, Numero, Data_Emissao, UF_Emitente, CFOP, Per_ICMS, Per_ICMS10,Per_ICMS51,Per_ICMS20,per_icmsouf,Tipo_Doc
                    ORDER BY Numero
                """
                st.write("#### Registro de Entradas")
                st.data_editor(con.execute(query_entradas).df(), hide_index=True, use_container_width=True)
                
                # Métricas de Impostos
                df_metrics = con.execute("""
                                            SELECT 
                                                   ROUND(SUM(
                                                            COALESCE(Valor_Total, 0) + 
                                                            COALESCE(Vlr_ICMS_ST, 0) + 
                                                            COALESCE(Vlr_FCPST, 0) + 
                                                            COALESCE(Vlr_IPI, 0) + 
                                                            COALESCE(OutrasDespAces, 0)
                                                        ), 2) AS Vlr_Contabil,
                                                   ROUND(SUM(COALESCE(BC_ICMS, 0)+
                                                        COALESCE(BC_ICMS10,0)+
                                                        COALESCE(BC_ICMS51,0)+
                                                        COALESCE(BC_ICMS20,0)+
                                                        COALESCE(bc_icmsouf,0)), 2) AS bc_icms,
                                                   SUM(vlr_ICMS) as total_icms,
                                                   SUM(BC_ICMS_ST) as bc_icms_st,
                                                   SUM(vlr_icms_st) as vlr_icms_st,
                                                   SUM(vlr_fcpst) as vlr_fcpst,
                                                   SUM(Vlr_ICMS51) as vlr_icms_dif,
                                                   SUM(vlr_ipi) as vlr_ipi
                                            FROM df_uni_filtrado""").df().iloc[0]
                c1,c2,c3,c4 = st.columns(4)
                c1.metric(label='Total Valor Contabil', value=f"R${df_metrics['Vlr_Contabil']:,.2f}".replace(",","X")
                    .replace(".",",").replace("X", "."), border=True, width="content",)
                c2.metric(label='Total BC-ICMS', value=f"R${df_metrics['bc_icms']:,.2f}".replace(",","X")
                    .replace(".",",").replace("X", "."), border=True, width="content")
                c3.metric(label='Total ICMS', value=f"R${df_metrics['total_icms']:,.2f}".replace(",","X")
                    .replace(".",",").replace("X", "."), border=True, width="content")
                c4.metric(label='Total Valor IPI', value=f"R${df_metrics['vlr_ipi']:,.2f}".replace(",","X")
                    .replace(".",",").replace("X", "."), border=True, width="content")

                c5,c6,c7,c8 = st.columns(4)
                c5.metric(label='Total BC-ICMSST', value=f"R${df_metrics['bc_icms_st']:,.2f}".replace(",","X")
                    .replace(".",",").replace("X", "."), border=True, width="content")
                c6.metric(label='Total Valor ICMSST', value=f"R${df_metrics['vlr_icms_st']:,.2f}".replace(",","X")
                    .replace(".",",").replace("X", "."), border=True, width="content")
                c7.metric(label='Total Valor FCPST', value=f"R${df_metrics['vlr_fcpst']:,.2f}".replace(",","X")
                    .replace(".",",").replace("X", "."), border=True, width="content")
                c8.metric(label='Total Valor ICMS Diferido', value=f"R${df_metrics['vlr_icms_dif']:,.2f}".replace(",","X")
                    .replace(".",",").replace("X", "."), border=True, width="content")
            else:
                st.info("Relatórios fiscais dependem de dados de NFe.") 
        with tab_piscofins:
            tab_piscofins.html(
            """
            <div style="text-align": center;">
                    <h3>Relatório de PisCofins Resumo CST e CFOP</h3>
            </div>
            """)
            with st.expander("Observação"):
                st.write("Quando não preenchidos, os percentuais de Pis e Cofins serão 1,65% e 7,6% respectivamente")
            query_piscofins = """
                                SELECT
                                    CST_PIS_UNIFICADO as CST_PIS,
                                    CASE
                                        WHEN CFOP IN (6101,6102,6105) THEN 2102
                                        WHEN CFOP IN (5101,5102,5105) THEN 1102
                                        WHEN CFOP IN (5403) THEN 1403
                                        WHEN CFOP IN (5352) THEN 1352
                                        WHEN CFOP IN (6352) THEN 2352
                                        WHEN CFOP IN (5353) THEN 1353
                                        WHEN CFOP IN (6353) THEN 2353
                                        WHEN CFOP IN (5932) THEN 1932
                                        WHEN CFOP IN (6932) THEN 2932
                                        WHEN CFOP IN (6401,6403) THEN 2403
                                        WHEN CFOP IN (6920) THEN 2920
                                        WHEN CFOP IN (6949) THEN 2949
                                        WHEN CFOP IN (5920) THEN 1920
                                        WHEN CFOP IN (5949) THEN 1949
                                        END AS CFOP_ENTRADA,
                                        ROUND(SUM(COALESCE(BC_ICMS, 0)+
                                            COALESCE(BC_ICMS51,0)+
                                            COALESCE(BC_ICMS20,0)+
                                            COALESCE(BC_ICMS10,0)+
                                            COALESCE(bc_icmsouf,0)), 2) AS BC_ICMS,
                                        ROUND(SUM(
                                                COALESCE(Valor_Total, 0) + 
                                                COALESCE(Vlr_ICMS_ST, 0) + 
                                                COALESCE(Vlr_FCPST, 0) + 
                                                COALESCE(Vlr_IPI, 0) + 
                                                COALESCE(OutrasDespAces, 0)
                                            ), 2) AS Vlr_Contabil,
                                        ROUND(SUM(COALESCE(vlr_ICMS, 0)+
                                                COALESCE(Vlr_ICMS51,0)+
                                                COALESCE(Vlr_ICMS20,0)+
                                                COALESCE(vlr_icms10,0)+
                                                COALESCE(vlr_icmsouf,0)), 2) AS Vlr_ICMS,
                                        ROUND(SUM(CASE
                                                    WHEN CST_PIS_UNIFICADO IN ('00','04','06','07','08','09','49','99')
                                                        THEN GREATEST(
                                                            COALESCE(valor_total,0) - COALESCE(vlr_icms,0) - COALESCE(vlr_icms51,0) - COALESCE(vlr_icmsouf,0)- COALESCE(vlr_icms10,0),
                                                            0
                                                        )
                                                    ELSE COALESCE(bc_pis,0)
                                            END),2) AS bc_pis,
                                        ROUND(
                                            SUM(
                                                CASE
                                                    WHEN Vlr_Pis IS NOT NULL
                                                        THEN Vlr_Pis
                                                    ELSE
                                                        (
                                                            CASE
                                                                WHEN CST_PIS_UNIFICADO IN ('00','04','06','07','08','09','49','99')
                                                                    THEN GREATEST(
                                                                        COALESCE(valor_total,0)
                                                                        - COALESCE(vlr_icms,0)
                                                                        - COALESCE(vlr_icms51,0)
                                                                        - COALESCE(vlr_icms10,0),
                                                                        0
                                                                    )
                                                                ELSE COALESCE(bc_pis,0)
                                                            END
                                                        ) * 0.0165
                                                END
                                            ),
                                            2
                                        ) AS vlr_pis,
                                        ROUND(SUM(CASE
                                                    WHEN CST_PIS_UNIFICADO IN ('00','04','06','07','08','09','49','99')
                                                        THEN GREATEST(
                                                            COALESCE(valor_total,0) - COALESCE(vlr_icms,0) - COALESCE(vlr_icms51,0)- COALESCE(vlr_icmsouf,0) - COALESCE(vlr_icms10,0),
                                                            0
                                                        )
                                                    ELSE COALESCE(bc_pis,0)
                                            END),2) AS bc_cofins,
                                        ROUND(
                                                SUM(
                                                    CASE
                                                        WHEN Vlr_Cofins IS NOT NULL
                                                            THEN Vlr_Cofins
                                                        ELSE
                                                            (
                                                                CASE
                                                                    WHEN CST_PIS_UNIFICADO IN ('00','04','06','07','08','09','49','99')
                                                                        THEN GREATEST(
                                                                            COALESCE(valor_total,0)
                                                                            - COALESCE(vlr_icms,0)
                                                                            - COALESCE(vlr_icms51,0)
                                                                            - COALESCE(vlr_icms10,0),
                                                                            0
                                                                        )
                                                                    ELSE COALESCE(bc_pis,0)
                                                                END
                                                            ) * 0.076
                                                    END
                                                ),
                                                2
                                            ) AS vlr_cofins
                                        FROM
                                            (SELECT 
                                                Numero,Chave_Acesso,CST_PIS_UNIFICADO,bc_icms,
                                                CFOP, Valor_Total, Vlr_ICMS,vlr_icms_st, vlr_fcpst, bc_pis,
                                                BC_ICMS51,Vlr_ICMS51,BC_iCMS20,vlr_icms20, BC_ICMSOUF,vlr_icmsouf, bc_icms10, per_icms10, vlr_icms10,
                                                Per_Pis,vlr_pis,vlr_cofins, bc_pis,bc_cofins, vlr_cofins, OutrasDespAces, vlr_ipi
                                        FROM df_uni_filtrado
                                        )
                                        WHERE CFOP NOT IN (5151,5152,6151,6152,6949,5949,5920,6920,5910,6910)
                                        GROUP BY CFOP_ENTRADA, CST_PIS_UNIFICADO,per_pis
                                        ORDER BY CFOP_ENTRADA, CST_PIS_UNIFICADO
                            """
            query_piscofins_analitico = f"""
                                            SELECT
                                                numero,
                                                nome_emitente,
                                                CST_PIS_UNIFICADO as CST_PIS,
                                                CASE
                                                    WHEN CFOP IN (6101,6102,6105) THEN 2102
                                                    WHEN CFOP IN (5101,5102,5105) THEN 1102
                                                    WHEN CFOP IN (5403) THEN 1403
                                                    WHEN CFOP IN (5352) THEN 1352
                                                    WHEN CFOP IN (6352) THEN 2352
                                                    WHEN CFOP IN (5353) THEN 1353
                                                    WHEN CFOP IN (6353) THEN 2353
                                                    WHEN CFOP IN (5932) THEN 1932
                                                    WHEN CFOP IN (6932) THEN 2932
                                                    WHEN CFOP IN (6401,6403) THEN 2403
                                                    WHEN CFOP IN (6920) THEN 2920
                                                    WHEN CFOP IN (6949) THEN 2949
                                                    WHEN CFOP IN (5920) THEN 1920
                                                    WHEN CFOP IN (5949) THEN 1949
                                                END AS CFOP_ENTRADA,
                                                ncm,
                                                nome_produto,
                                                bc_icms,
                                                vlr_icms,
                                                ROUND(CASE
                                                    WHEN CST_PIS_UNIFICADO IN ('00','04','06','07','08','09','49','99')
                                                        THEN GREATEST(
                                                            COALESCE(valor_total,0) - COALESCE(vlr_icms,0) - COALESCE(vlr_icms51,0),
                                                            0
                                                    )
                                                    ELSE COALESCE(bc_pis,0)
                                                END) AS bc_pis,
                                                per_pis,
                                                vlr_pis,
                                                bc_cofins,
                                                per_cofins,
                                                vlr_cofins
                                            FROM df_uni_filtrado
                                            WHERE CFOP NOT IN (5151,5152,6151,6152,6949,5949,5920,6920,5910,6910)
                                    """
            metrics_piscofins = f"""
                                    SELECT
                                        ROUND(SUM(
                                                COALESCE(Valor_Total, 0) + 
                                                COALESCE(Vlr_ICMS_ST, 0) + 
                                                COALESCE(Vlr_FCPST, 0) + 
                                                COALESCE(Vlr_IPI, 0) 
                                                --COALESCE(OutrasDespAces, 0)
                                            ), 2) AS Vlr_Contabil,
                                        ROUND(SUM(COALESCE(vlr_ICMS, 0)+
                                                COALESCE(Vlr_ICMS51,0)+
                                                COALESCE(Vlr_ICMS20,0)+
                                                COALESCE(vlr_icms10,0)+
                                                COALESCE(vlr_icmsouf,0)), 2) AS Total_ICMS,
                                        ROUND(SUM(CASE
                                                    WHEN CST_PIS_UNIFICADO IN ('00','04','06','07','08','09','49','99')
                                                        THEN GREATEST(
                                                            COALESCE(valor_total,0) - COALESCE(vlr_icms,0) - COALESCE(vlr_icms51,0) - COALESCE(vlr_icms10,0) - COALESCE(Vlr_IPI,0)-COALESCE(vlr_icmsouf,0),
                                                            0
                                                        )
                                                    ELSE COALESCE(bc_pis,0)
                                            END),2) AS bc_pis,
                                        ROUND(
                                            SUM(
                                                CASE
                                                    WHEN Vlr_Pis IS NOT NULL
                                                        THEN Vlr_Pis
                                                    ELSE
                                                        (
                                                            CASE
                                                                WHEN CST_PIS_UNIFICADO IN ('00','04','06','07','08','09','49','99')
                                                                    THEN GREATEST(
                                                                        COALESCE(valor_total,0)
                                                                        - COALESCE(vlr_icms,0)
                                                                        - COALESCE(vlr_icms51,0)
                                                                        - COALESCE(vlr_icms10,0),
                                                                        0
                                                                    )
                                                                ELSE COALESCE(bc_pis,0)
                                                            END
                                                        ) * 0.0165
                                                END
                                            ),
                                            2
                                        ) AS vlr_pis,
                                        ROUND(SUM(CASE
                                                    WHEN CST_PIS_UNIFICADO IN ('00','04','06','07','08','09','49','99')
                                                        THEN GREATEST(
                                                            COALESCE(valor_total,0) - COALESCE(vlr_icms,0) - COALESCE(vlr_icms51,0) - COALESCE(vlr_icms10,0) - COALESCE(Vlr_IPI, 0)-COALESCE(vlr_icmsouf,0),
                                                            0
                                                        )
                                                    ELSE COALESCE(bc_cofins,0)
                                            END),2) AS bc_cofins,
                                        ROUND(
                                            SUM(
                                                CASE
                                                    WHEN Vlr_Cofins IS NOT NULL
                                                        THEN Vlr_Cofins
                                                    ELSE
                                                        (
                                                            CASE
                                                                WHEN CST_PIS_UNIFICADO IN ('00','04','06','07','08','09','49','99')
                                                                    THEN GREATEST(
                                                                        COALESCE(valor_total,0)
                                                                        - COALESCE(vlr_icms,0)
                                                                        - COALESCE(vlr_icms51,0)
                                                                        -COALESCE(vlr_icms10,0),
                                                                        0
                                                                    )
                                                                ELSE COALESCE(bc_cofins,0)
                                                            END
                                                        ) * 0.076
                                                END
                                            ),
                                            2
                                        ) AS vlr_cofins,
                                        ROUND(SUM(Vlr_ICMS_ST),2) AS Vlr_ICMS_ST,
                                        ROUND(SUM(Vlr_FCPST),2) AS Vlr_FCPST,
                                        ROUND(SUM(Vlr_IPI),2) AS Vlr_IPI,
                                    FROM df_uni_filtrado
                                    WHERE CFOP NOT IN (5151,5152,6151,6152,6949,5949,5920,6920,5910,6910)
                            """
            df_metrics = duckdb.sql(metrics_piscofins).df().iloc[0]

            c1, c2, c3, c4,c5 = st.columns(5)
            c1.metric(label='Total Valor Contabil', value=f"R${df_metrics['Vlr_Contabil']:,.2f}".replace(",", "X")
                      .replace(".", ",").replace("X", "."), border=True, width="content", )
            c2.metric(label='Total ICMS', value=f"R${df_metrics['Total_ICMS']:,.2f}".replace(",", "X")
                      .replace(".", ",").replace("X", "."), border=True, width="content")
            c3.metric(label='Total ICMS-ST', value=f"R${df_metrics['Vlr_ICMS_ST']:,.2f}".replace(",", "X")
                      .replace(".", ",").replace("X", "."), border=True, width="content")
            c4.metric(label='Total FCP-ST', value=f"R${df_metrics['Vlr_FCPST']:,.2f}".replace(",", "X")
                      .replace(".", ",").replace("X", "."), border=True, width="content")
            c5.metric(label='Total IPI', value=f"R${df_metrics['Vlr_IPI']:,.2f}".replace(",", "X")
                      .replace(".", ",").replace("X", "."), border=True, width="content")
            c6,c7,c8,c9 = st.columns(4)
            c6.metric(label='Total BC PIS', value=f"R${df_metrics['bc_pis']:,.2f}".replace(",", "X")
                      .replace(".", ",").replace("X", "."), border=True, width="content")
            c7.metric(label='Total Valor PIS', value=f"R${df_metrics['vlr_pis']:,.2f}".replace(",", "X")
                      .replace(".", ",").replace("X", "."), border=True, width="content")
            c8.metric(label='Total Base Cofins', value=f"R${df_metrics['bc_cofins']:,.2f}".replace(",", "X")
                      .replace(".", ",").replace("X", "."), border=True, width="content")
            c9.metric(label='Total Valor Cofins', value=f"R${df_metrics['vlr_cofins']:,.2f}".replace(",", "X")
                      .replace(".", ",").replace("X", "."), border=True, width="content")

            st.data_editor(con.execute(query_piscofins).df(), hide_index=True, use_container_width=True)
            st.data_editor(con.execute(query_piscofins_analitico).df(), hide_index=True, use_container_width=True)

        with tab_ref_trib:
            tab_ref_trib.html(
            """
            <div style="text-align": center;">
                    <h3>Relatório Analítico de IBC-CBS</h3>
            </div>
            """)

            query_ref_trib = """
                                SELECT
                                    numero,
                                    chave_acesso,
                                    data_emissao,
                                    Nome_Emitente,
                                    nome_produto,
                                    ncm,
                                    cfop,
                                    cst_pis_unificado,
                                    valor_total,
                                    cst_ibscbs,
                                    clas_trib_ibscbs,
                                    bc_icms,
                                    vlr_icms,
                                    vlr_ipi,
                                    vlr_pis,
                                    vlr_cofins,
                                    bc_ibscbs,
                                    per_cbs,
                                    vlr_cbs,
                                    per_ibsuf,
                                    vlr_ibsuf
                                FROM df_uni_filtrado

                            """
            metrics_rtc = """
                            SELECT
                                cfop,
                                CST_PIS_UNIFICADO as cst_piscofins,
                                cst_ibscbs,
                                clas_trib_ibscbs,
                                ROUND(SUM(
                                                COALESCE(Valor_Total, 0) + 
                                                COALESCE(Vlr_ICMS_ST, 0) + 
                                                COALESCE(Vlr_FCPST, 0) + 
                                                COALESCE(Vlr_IPI, 0) + 
                                                COALESCE(OutrasDespAces, 0)
                                            ), 2) AS Vlr_Contabil,
                                ROUND(SUM(COALESCE(vlr_ICMS, 0)+
                                                COALESCE(Vlr_ICMS51,0)+
                                                COALESCE(Vlr_ICMS20,0)+
                                                COALESCE(vlr_icms10,0)+
                                                COALESCE(vlr_icmsouf,0)), 2) AS Total_ICMS,
                                                ROUND(
                                            SUM(
                                                CASE
                                                    WHEN Vlr_Pis IS NOT NULL
                                                        THEN Vlr_Pis
                                                    ELSE
                                                        (
                                                            CASE
                                                                WHEN CST_PIS_UNIFICADO IN ('00','04','06','07','08','09','49','99')
                                                                    THEN GREATEST(
                                                                        COALESCE(valor_total,0)
                                                                        - COALESCE(vlr_icms,0)
                                                                        - COALESCE(vlr_icms51,0),
                                                                        0
                                                                    )
                                                                ELSE COALESCE(bc_pis,0)
                                                            END
                                                        ) * 0.0165
                                                END
                                            ),
                                            2
                                        ) AS vlr_pis,
                                ROUND(
                                            SUM(
                                                CASE
                                                    WHEN Vlr_Cofins IS NOT NULL
                                                        THEN Vlr_Cofins
                                                    ELSE
                                                        (
                                                            CASE
                                                                WHEN CST_PIS_UNIFICADO IN ('00','04','06','07','08','09','49','99')
                                                                    THEN GREATEST(
                                                                        COALESCE(valor_total,0)
                                                                        - COALESCE(vlr_icms,0)
                                                                        - COALESCE(vlr_icms51,0),
                                                                        0
                                                                    )
                                                                ELSE COALESCE(bc_cofins,0)
                                                            END
                                                        ) * 0.076
                                                END
                                            ),
                                            2
                                        ) AS vlr_cofins,
                                ROUND(SUM(vlr_ipi),2) as valor_ipi,
                                ROUND(SUM(bc_ibscbs),2) as base_ibscbs,
                                ROUND(SUM(vlr_cbs),2) as valor_cbs,
                                ROUND(SUM(vlr_ibsuf),2) as valor_ibs_uf
                            FROM df_uni_filtrado
                            GROUP BY cfop,cst_piscofins,cst_ibscbs,clas_trib_ibscbs
                            ORDER BY cst_ibscbs
                        """

            
            
            st.data_editor(con.execute(query_ref_trib).df(), hide_index=True, use_container_width=True)
            st.data_editor(con.execute(metrics_rtc).df(),hide_index=True, width='content')

    else:
        st.info("Aguardando upload de arquivos XML para iniciar...")

if __name__ == "__main__":
    main()
