import streamlit as st
import streamlit_authenticator as stauth

# Carregar usuários do secrets
names = st.secrets["NAMES"]
usernames = st.secrets["USERNAMES"]
passwords = st.secrets["PASSWORDS"]

# Criptografar senhas
hashed_passwords = stauth.Hasher(passwords).generate()

# Criar o autenticador
authenticator = stauth.Authenticate(
    names, usernames, hashed_passwords,
    'meu_app', 'chave_cookie_segura', cookie_expiry_days=1
)

# Exibir login
name, auth_status, username = authenticator.login('Login', 'main')

if auth_status:
    st.success(f'Bem-vindo {name}')
    # Aqui entra o resto do seu app
elif auth_status is False:
    st.error('Usuário ou senha incorretos')
else:
    st.warning('Digite suas credenciais para acessar o app')


pagina_1 = st.Page("2-leitor_xml_com_duckdb.py", title="Converter Arquivos XML para Tabela")
pagina_2 = st.Page("3-leitor_pdf.py", title="Conferência Autorizações de Uso NFe")
pg = st.navigation([pagina_1, pagina_2])
pg.run()





