import os
import pandas as pd
import requests
from urllib.parse import urlencode
import re

# -------------------------------------------- FUNCIONES ------------------------------------------------------ #

def delete_file(filepath):
    if os.path.isfile(filepath):
        os.remove(filepath)
        print("Archivo eliminado con éxito.")
    else:
        print("El archivo no existe.")

# --------------------------------------------------------------------------------------------------------------- #   

def extract_info_api(url):
    username = "email@gmail.com"
    password = "Password"
    limit = 10000
    params = urlencode({"$limit": limit})
    url_read = f"{url}?{params}"
    response = requests.get(url_read, auth=(username, password))

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        return df
    else:
        print("Error en la solicitud a la API:", response.status_code)

# --------------------------------------------------------------------------------------------------------------- #

def extract_full_url(text):
    match = re.search(r'https?://[^\'\s]+', text)
    if match:
        return match.group(0)
    else:
        return None

# --------------------------------------------------------------------------------------------------------------- #

def extract_url(text):
    match = re.search(r'https?://[^\s]+isFromPublicArea=True&isModal=true&asPopupView=true', text)
    if match:
        return match.group(0)
    else:
        return None

# --------------------------------------------------------------------------------------------------------------- #

def extract_twonumbers_after_dot(text):
    pattern = r"\.(\d{2})"
    matches = re.findall(pattern, text)
    if matches:
        return matches[0]
    return None

# --------------------------------------------------------------------------------------------------------------- #

def extract_fournumbers_after_dot(text):
    pattern = r"\.(\d{4})"
    matches = re.findall(pattern, text)
    if matches:
        return matches[0]
    return None

# --------------------------------------------------------------------------------------------------------------- #

def extract_sixnumbers_after_dot(text):
    pattern = r"\.(\d{6})"
    matches = re.findall(pattern, text)
    if matches:
        return matches[0]
    return None

# --------------------------------------------------------------------------------------------------------------- #

def extract_first_two_numbers(text):
    return text[:2]