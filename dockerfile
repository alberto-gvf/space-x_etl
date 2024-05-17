FROM python:3.9

COPY notebooks/ /notebooks/
COPY requirements-docker.txt /requirements.txt

RUN apt-get update && apt-get install -y gcc python3-dev postgresql postgresql-client
RUN pip install -I -r /requirements.txt
RUN pip install jupyter

WORKDIR /notebooks

EXPOSE 8080

CMD ["jupyter", "notebook", "--ip='0.0.0.0'", "--port=8080", "--no-browser", "--allow-root", "--NotebookApp.token=''"]
