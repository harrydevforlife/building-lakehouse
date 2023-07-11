#!/bin/sh

cd /home/app/
export PYTHONPATH=/home/app/
streamlit run core/1_🏠_Logout.py --server.port 8051 --server.headless true
