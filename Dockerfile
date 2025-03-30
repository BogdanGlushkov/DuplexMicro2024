FROM python:3.12-slim

COPY req.txt req.txt
RUN pip install -r req.txt

COPY . .

CMD ["python", "main.py"]