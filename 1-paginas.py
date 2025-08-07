import streamlit as st
import streamlit_authenticator as stauth
import importlib

# Configurar login
names = st.secrets["NAMES"]
usernames = st.secrets["USERNAMES"]
passwords = st.secrets["PASSWORDS"]

hashed_passwords = stauth.Hasher(passwords).generate()

authenticator = stauth.Authenticate(
    names, usernames, hashed_passwords,
    "meu_app", "cookie_key", cookie_expiry_days=1
)

name, auth_status, username = authenticator.login("Login", "main")

# Verifica status de autenticação
if auth_status:
    st.sidebar.success(f"Bem-vindo, {name}")
    
    # Aqui você define e exibe as páginas
    st.sidebar.title("Navegação")
    pagina = st.sidebar.radio("Escolha a página", ["Leitor XML", "Outra funcionalidade"])

    if pagina == "Leitor XML":
        # Importa funcionalidades da ferramenta
        leitor = importlib.import_module("2-leitor_xml.py")
        leitor.exibir()  # Assumindo que você tem uma função chamada exibir() no outro arquivo

elif auth_status is False:
    st.error("Usuário ou senha incorretos")

else:
    st.warning("Digite suas credenciais para acessar")







