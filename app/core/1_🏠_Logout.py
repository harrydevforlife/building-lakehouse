import streamlit as st
from core.auth.authenticator import Auth

# emojis: https://www.webfx.com/tools/emoji-cheat-sheet/
st.set_page_config(page_title="Recommendation System", page_icon=":bar_chart:", layout="wide")

class HomePage:
    def __init__(self):
        self._authenticator = Auth()

    def _show_home_page(self):
        st.title('Home page')
        st.write('This is the home page')

    def show(self):
        self._show_home_page()

    def main(self):
        '''
        First look of user is login page, if login success, go to home page
        else stay in login page
        
        '''

        hide_bar= """
            <style>
            [data-testid="stSidebar"][aria-expanded="true"] > div:first-child {
                visibility:hidden;
                width: 0px;
            }
            [data-testid="stSidebar"][aria-expanded="false"] > div:first-child {
                visibility:hidden;
            }
            </style>
        """

        authentication_status = self._authenticator.login()

        if authentication_status:
            self.show() 

            hide_st_style = """
                        <style>
                        #MainMenu {visibility: hidden;}
                        footer {visibility: hidden;}
                        header {visibility: hidden;}
                        </style>
                        """
            st.markdown(hide_st_style, unsafe_allow_html=True)

            print(st.session_state['username'])
            print(st.session_state['userid'])

        elif authentication_status == False:
            st.error('Username/password is incorrect')
            st.markdown(hide_bar, unsafe_allow_html=True)

        elif authentication_status == None:
            st.warning('Please enter your username and password')
            st.markdown(hide_bar, unsafe_allow_html=True)


if __name__ == '__main__':  
    home = HomePage()
    home.main()

