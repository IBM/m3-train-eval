# invocable-api-hub

### Set up a dev env
`virtualenv .venv`

`source .venv/bin/activate`

### Install requirements
`pip install -r requirements.txt`

### To start the server

`PYTHONPATH=. uvicorn server:app --reload`

Navigate to view the openapi spec, http://127.0.0.1:8000/openapi.json

To view the swagger docs, go to http://127.0.0.1:8000/docs

## Deploy to code engine

Make sure you have access to RIS3 cloud account and Routing namespace - if not, ping Anupama Murthi about it.

Have bluemix / ibmcloud CLI set up in your local machine

`bx login --sso`

Pick RIS3

`bx cr login`

Build the docker image. Note, if you are on M1, specify the platform as seen below 

`docker buildx build --platform=linux/amd64 -t invocable-api-hub .`

Otherwise, the below should work

`docker buildx build -t invocable-api-hub`

Tag and Push the image

`docker tag invocable-api-hub icr.io/routing_namespace/invocable-api-hub`

`docker push icr.io/routing_namespace/invocable-api-hub`

Navigate to 
https://cloud.ibm.com/codeengine/project/us-east/e214613f-fc72-4d58-86cf-3f292756a743/applications

