// Databricks notebook source
// MAGIC %python
// MAGIC from datetime import datetime
// MAGIC from datetime import date, timedelta
// MAGIC import sys
// MAGIC
// MAGIC import requests
// MAGIC from pandas import json_normalize
// MAGIC import numpy as np
// MAGIC import pandas as pd
// MAGIC import os
// MAGIC import requests
// MAGIC import pandas as pd
// MAGIC import time
// MAGIC
// MAGIC data_df_inicio = date.today - timedelta(days=1)
// MAGIC
// MAGIC sysparm_table = 'sn_customerservice_case_dga'
// MAGIC sysparm_fields = 'number, u_krotonpad_treeview'
// MAGIC sysparm_limit = 10000
// MAGIC sysparm_query = f"""sys_created_byLIKE-ate^sys_created_onBETWEENjavascript:gs.dateGenerate('{data_df_inicio}','00:00:00')@javascript:gs.dateGenerate('{data_df_inicio}','00:59:59')^u_canal_entradaLIKETelefone"""       
// MAGIC
// MAGIC username = 'lucas.mg'
// MAGIC password = '123'
// MAGIC url = f"https://kroton.service-now.com/api/now/table/{sysparm_table}"
// MAGIC headers = {"Content-Type": "application/json", "Accept": "application/json"}
// MAGIC query_params = {
// MAGIC     "sysparm_fields": sysparm_fields,
// MAGIC     "sysparm_query": sysparm_query,
// MAGIC     "sysparm_display_value": "true",
// MAGIC     "sysparm_limit": sysparm_limit,
// MAGIC     "sysparm_exclude_reference_link": "true",
// MAGIC     "sysparm_suppress_pagination_header": "true"
// MAGIC }   
// MAGIC
// MAGIC response = requests.get(url, auth=(username, password), headers=headers, params=query_params)
// MAGIC response.raise_for_status()
// MAGIC data = response.json()
// MAGIC result = pd.json_normalize(data["result"])
// MAGIC
