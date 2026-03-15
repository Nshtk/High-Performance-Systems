FROM python:3.13-slim

RUN pip install --no-cache-dir jupyter
WORKDIR /workspace

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY notebooks/ ./notebooks/
COPY regular/ ./regular/
EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]