FROM jnj.artifactrepo.jnj.com/python:3.9.7

WORKDIR /app
COPY . /app
RUN pip3 install -r requirements.txt

EXPOSE 8000
CMD [ "sh", "start_api.sh"]