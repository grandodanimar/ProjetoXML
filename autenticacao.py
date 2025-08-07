import streamlit as st

def login():
    st.title("🔐 Login")

    if "autenticado" not in st.session_state:
        st.session_state.autenticado = False

    usuario = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")

    # Acessa os dados do secrets
    usuarios = st.secrets["auth"]

    if st.button("Entrar"):
        if usuario in usuarios and usuarios[usuario] == senha:
            st.session_state.autenticado = True
            st.session_state.usuario = usuario
            st.success("Login bem-sucedido!")
        else:
            st.error("Usuário ou senha incorretos.")
