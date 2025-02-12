import os
import json
import openai
import uvicorn
import pandas as pd
import xes
from fastapi import FastAPI, Query
from sentence_transformers import SentenceTransformer, util
from pinecone import Pinecone

app = FastAPI()

# Load the embedding model
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

# Connect to Pinecone
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
index = pc.Index("event_logs")

def retrieve_relevant(query, top_k=5):
    query_embedding = embedding_model.encode(query).tolist()
    results = index.query(query_embedding, top_k=top_k, include_metadata=True)
    return [res["metadata"] for res in results]

def generate_answer(query, retrieved_data):
    context = "\n".join(json.dumps(entry) for entry in retrieved_data)
    prompt = f"""
    Context:
    {context}

    Question: {query}

    Answer:
    """
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "system", "content": "You are an expert in process mining."}, {"role": "user", "content": prompt}]
    )
    return response["choices"][0]["message"]["content"]

@app.get("/query")
def query_rag(question: str):
    retrieved_data = retrieve_relevant(question)
    answer = generate_answer(question, retrieved_data)
    return {"query": question, "retrieved_data": retrieved_data, "generated_answer": answer}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
