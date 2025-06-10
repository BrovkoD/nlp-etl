from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from transformers import T5Tokenizer, T5ForConditionalGeneration

app = FastAPI()

MODEL_PATH = "t5-small"
tokenizer = T5Tokenizer.from_pretrained(MODEL_PATH)
model = T5ForConditionalGeneration.from_pretrained(MODEL_PATH)


class TextInput(BaseModel):
    text: str


class PredictionOutput(BaseModel):
    summary: str


@app.post("/predict", response_model=PredictionOutput)
def predict(input_data: TextInput):
    try:
        input_text = f"summarize: {input_data.text}"
        inputs = tokenizer.encode(input_text, return_tensors="pt", max_length=512, truncation=True)

        summary_ids = model.generate(
            inputs,
            max_length=150,
            min_length=30,
            length_penalty=2.0,
            num_beams=4,
            early_stopping=True
        )

        summary = tokenizer.decode(summary_ids[0], skip_special_tokens=True)
        return {"summary": summary}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
