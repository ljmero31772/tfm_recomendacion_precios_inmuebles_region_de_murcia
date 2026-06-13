import os
import json
import logging
import time
import ast
import re
from typing import Dict, Any, List
from openai import OpenAI

log = logging.getLogger(__name__)

# Configuración obtenida del entorno
FIREWORKS_AI_TOKEN = os.environ.get('FIREWORKS_AI_TOKEN')
FIREWORKS_AI_BASE_URL = os.environ.get('FIREWORKS_AI_BASE_URL')
FIREWORKS_AI_MODEL = os.environ.get('DATABRICKS_AI_GATEWAY_ENDPOINT')

_client = None

def get_client() -> OpenAI:
    """
    Instancia el cliente de OpenAI configurado para atacar a Fireworks AI.
    """
    global _client
    if _client is None:
        if not FIREWORKS_AI_TOKEN or not FIREWORKS_AI_BASE_URL or not FIREWORKS_AI_MODEL:
            raise ValueError("FIREWORKS_AI_TOKEN o FIREWORKS_AI_BASE_URL o FIREWORKS_AI_MODEL no están configurados en el entorno.")
        
        _client = OpenAI(
            api_key=FIREWORKS_AI_TOKEN,
            base_url=FIREWORKS_AI_BASE_URL
        )
    return _client


def query_json_to_fireworks_ai(prompt: str, expected_keys: List[str]):
    """
    Lanza un prompt al servicio de Fireworks AI y devolvemos la respuesta como JSON.
    """
    model_name = FIREWORKS_AI_MODEL    
    client = get_client()
    
    #Sanitizamos el prompt: según el feedback, el modelo prefiere comillas simples 
    # o escapadas para no romper la estructura del JSON de la petición.
    current_prompt = prompt.replace('"', "'")

    #Llamada al modelo
    chat_completion=client.chat.completions.create(
                model=model_name,
                messages=[
                    {
                        "role": "user", 
                        "content": current_prompt
                    }
                ],
                max_tokens=5000,
                temperature=0.0#Lo dejamos en 0 para que sea más determinístico y no haya errores.
        )
    
    #Recuperamos la respuesta del modelo
    raw_content = chat_completion.choices[0].message.content

    #Devolvemos la respuesta tal cual del modelo, ya la procesaremos después
    return raw_content

