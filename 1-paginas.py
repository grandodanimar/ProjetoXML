import streamlit as st
import streamlit_authenticator as stauth

try:
    names = st.secrets["NAMES"]
    usernames = st.secrets["USERNAMES"]
    passwords = st.secrets["PASSWORDS"]

    if not isinstance(passwords, list):
        st.error("As senhas devem estar em uma lista. Corrija o secrets.toml.")
    else:
        hashed_passwords = stauth.Hasher(passwords).generate()

        authenticator = stauth.Authenticate(
            names, usernames, hashed_passwords,
            "meu_app", "cookie_key", cookie_expiry_days=1
        )

        name, auth_status, username = authenticator.login("Login", "main")

        if auth_status:
            st.success(f"Bem-vindo {name}!")
        elif auth_status is False:
            st.error("Usuário ou senha incorretos.")
        else:
            st.warning("Digite suas credenciais para acessar.")

except Exception as e:
    st.error(f"Erro na autenticação: {e}")
