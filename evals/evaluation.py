import os
import glob
import json
import requests
from numpy.linalg import norm
from numpy import dot

from dotenv import load_dotenv

load_dotenv(verbose=True)

# Configuration
API_URL = os.getenv("OPEN_WEB_UI_BASE_URL", None)
if API_URL is None:
    raise ValueError("API URL not set. Please set OPEN_WEB_UI_BASE_URL in your environment variables.")
else:
    API_URL += "/api/chat/completions"

API_KEY = os.getenv("OPEN_WEB_UI_API_KEY", None)
if API_KEY is None:
    raise ValueError("API Key not set. Please set OPEN_WEB_UI_API_KEY in your environment variables.")

MODEL_ID = os.getenv("EVALUATION_MODEL", None)
EVAL_FILES_PATH = "data/eval*.txt"
if MODEL_ID is None:
    raise ValueError("Model ID not set. Please set EVALUATION_MODEL in your environment variables.")

def get_files(path):
    return glob.glob(path)


def read_file(file_path):
    print(f"Reading file {file_path}..")
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()


def split_question_answer(content):
    parts = content.split('---')
    if len(parts) != 2:
        raise ValueError("File content is not in the expected format.")
    return parts[0].strip(), parts[1].strip()


def call_openwebui_api(question):
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    # Model should match to model ID, not model name
    data = {
        "model": MODEL_ID,
        "messages": [
            {
                "role": "user",
                "content": question
            }
        ]
    }
    print(data)

    response = requests.post(API_URL, headers=headers, json=data)
    response.raise_for_status()
    return response.json()


def main():
    files = get_files(EVAL_FILES_PATH)
    for file_path in files:
        try:
            content = read_file(file_path)
            question, model_answer = split_question_answer(content)
            api_response = call_openwebui_api(question)
            generated_answer = api_response['choices'][0]['message']['content']
            print(f"Question: {question}")
            print("-----------------------------")
            print(f"Model Answer: {model_answer}")
            print("-----------------------------")
            print(f"Generated Answer: {generated_answer}")
            print("-----------------------------")

            # Get embeddings
            headers = {"Authorization": f"Bearer {API_KEY}"}
            response = requests.get(os.getenv("OPEN_WEB_UI_BASE_URL") + f"/api/v1/retrieval/ef/{generated_answer}", headers=headers)
            response.raise_for_status()
            generated_answer_embeddings = response.json()["result"]
            print(generated_answer_embeddings)
            model_answer = model_answer.replace("/", "#")
            response = requests.get(os.getenv("OPEN_WEB_UI_BASE_URL") + f"/api/v1/retrieval/ef/{model_answer}", headers=headers)
            response.raise_for_status()
            model_answer_embeddings = response.json()["result"]
            # print(model_answer_embeddings)

            # Calculate score
            score = dot(model_answer_embeddings, generated_answer_embeddings)/(norm(model_answer_embeddings)*norm(generated_answer_embeddings))
            print("score: ", score)

            # Compare answers with LLM
            comparison_data = {
                "model": MODEL_ID,
                "messages": [
                    {
                        "role": "user",
                        "content": f"Compare the following two answers and rate them on a scale from 0 to 10, where 0 means they are completely different and 10 means they are semantically the same. Also give short feedback. Answer 1: {model_answer} Answer 2: {generated_answer}"
                    }
                ]
            }
            comparison_response = requests.post(API_URL, headers=headers, json=comparison_data)
            comparison_response.raise_for_status()
            comparison_result = comparison_response.json()
            print("Comparison Result: ", comparison_result['choices'][0]['message']['content'])

        except Exception as e:

            print(f"Error processing file {file_path}: {e}")


if __name__ == "__main__":
    main()