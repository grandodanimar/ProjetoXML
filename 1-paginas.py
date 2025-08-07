import streamlit as st
import streamlit_authenticator as stauth

names = st.secrets.get("NAMES", [])
usernames = st.secrets.get("USERNAMES", [])
passwords = st.secrets.get("PASSWORDS", [])

# Verificações básicas
if not isinstance(passwords, list):
    st.error("As senhas devem estar em formato de lista. Corrija seu secrets.toml")
    st.stop()

# Geração de hash
hashed_passwords = stauth.Hasher(passwords).generate()

# Autenticador
authenticator = stauth.Authenticate(
    names,
    usernames,
    hashed_passwords,
    "meu_app", "cookie_key", cookie_expiry_days=1
)

name, auth_status, username = authenticator.login("Login", "main")

if auth_status:
    st.success(f"Bem-vindo {name}!")
    # Aqui vai seu app
elif auth_status is False:
    st.error("Usuário ou senha incorretos.")
else:
    st.warning("Digite suas credenciais para acessar.")
