import streamlit as st
from dataclasses import dataclass
import time
from call_llm import perform_llm_call
import pandas as pd
import base64
import plotly.graph_objects as go #---------------add

USER = "user"
ASSISTANT = "ai"
MESSAGES = "messages"

@dataclass
class Message:
    actor: str
    payload: str

@st.cache_data 
def get_img_as_base64(file):
    with open(file, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

img = get_img_as_base64("Resources\\BG2.png")
imgheader=get_img_as_base64("Resources\\veracitiz.jpg")

page_bg_img = """
<style>
    [data-testid="stAppViewContainer"] {
        background-image: url("data:image/png;base64,%s");
        background-size: cover;
        background-color: rgba(0,0,0,0);
        background-position: center;
    }
    [data-testid="stHeader"]{
    background-color: rgba(0,0,0,0);
    }
    .st-emotion-cache-u6ktf4 {
    background-color: rgba(0,0,0,0);
    }
</style>
""" % img

st.markdown(page_bg_img, unsafe_allow_html=True)

st.markdown(
    f'<img src="data:image/png;base64,{imgheader}" width="550" style="display: block; margin-left: auto; margin-right: auto;">',
    unsafe_allow_html=True
)

st.markdown("<h2 style='color: black; text-align: center;'>Unleash the Power of Watson<span style='color: #0000FF;'>x</span>.ai</h2>", unsafe_allow_html=True)

start_msg = "Hi, I am DataGuru 🧙‍♂️, I'm here to guide you through your data and help you find the answers you need. What can I help you with today?"

if MESSAGES not in st.session_state:
    st.session_state[MESSAGES] = [Message(actor=ASSISTANT, payload=start_msg)]

for msg in st.session_state[MESSAGES]:    # replace complete for loop
    if isinstance(msg, pd.DataFrame):
        st.dataframe(msg)
    elif isinstance(msg,go.Figure):
        st.plotly_chart(msg, use_container_width=True)
    else:
        st.chat_message(msg.actor, avatar='Resources\\wizard.png' if msg.actor == ASSISTANT else 'Resources\\user.png').write(msg.payload)

prompt = st.chat_input("Enter a prompt here")

if prompt:
    st.session_state[MESSAGES].append(Message(actor=USER, payload=prompt))
    st.chat_message(USER, avatar='Resources\\user.png').write(prompt)

    with st.spinner("Loading..."):
        response, new_data, graph_img, generated_response = perform_llm_call(prompt)
        response_text = ""
        chunk_size = 50  
        for i in range(0, len(response), chunk_size):
            chunk = response[i:i+chunk_size]
            response_text += chunk
            time.sleep(0.05)

        st.session_state[MESSAGES].append(Message(actor=ASSISTANT, payload=response_text))
        st.chat_message(ASSISTANT, avatar='Resources\\wizard.png').write(response_text)

    if isinstance(new_data, pd.DataFrame): # replace if
        st.dataframe(new_data)
        st.session_state[MESSAGES].append(new_data)
    if isinstance(graph_img,go.Figure):  # replace if
        st.plotly_chart(graph_img, use_container_width=True)
        st.session_state[MESSAGES].append(graph_img)

    code_container = st.empty()
    code_text = ""
    chunk_size = 10 
    for i in range(0, len(generated_response), chunk_size):
        chunk = generated_response[i:i+chunk_size]
        code_text += chunk
        code_container.code(code_text, language='python')
        time.sleep(0.05)  

    code_container.code(generated_response, language='python')

