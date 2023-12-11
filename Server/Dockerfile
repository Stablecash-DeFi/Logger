FROM python:3.9

WORKDIR /app
RUN pip install --no-cache-dir bottle pymongo

COPY app.py .
EXPOSE 8080

CMD ["python", "app.py"]
