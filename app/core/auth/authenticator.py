
from pyhive import hive
import streamlit as st



class Auth:
    def __init__(self):
        self._db_connection = self._connect_db()

    def _get_user_name(self, account_name: str):
        cursor = self._db_connection.cursor()
        cursor.execute(f"SELECT DISTINCT(account_name) FROM bronze.user_account WHERE account_name = '{account_name}'")
        result = cursor.fetchone()
        cursor.close()
        if result is not None:
            return result[0]
        else:
            return None 


    def _get_user_id(self, account_name: str):
        cursor = self._db_connection.cursor()
        cursor.execute(f"SELECT DISTINCT(userid) FROM bronze.user_account WHERE account_name = '{account_name}'")
        result = cursor.fetchone()
        cursor.close()
        if result is not None:
            return result[0]
        else:
            return None 


    def _connect_db(self, host='35.208.0.141', port=10000, username='admin'):
        conn = hive.Connection(host=host, port=port, username=username)
        return conn

    def login(self) -> tuple:
        '''
        Login page for user to login to system and get username and password from user input 

        :param button_text: text of button
        :param key: key of button

        :return: name, authentication_status, username

        '''
        placeholder = st.empty()
        if 'username' in st.session_state:
            del st.session_state['username']
        if 'userid' in st.session_state:
            del st.session_state['userid']


        # Insert a form in the container
        with placeholder.form("login", clear_on_submit = True):
            st.markdown("#### Enter your credentials")
            user = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit = st.form_submit_button("Login")


        username = self._get_user_name(user)

        if submit and user == username and password == user:
            placeholder.empty()
            if 'username' not in st.session_state:
                st.session_state['username'] = username

            if 'userid' not in st.session_state:
                st.session_state['userid'] = self._get_user_id(user)

            return True

        elif submit and user != username or password != user:
            st.error("Login failed")
            return False


