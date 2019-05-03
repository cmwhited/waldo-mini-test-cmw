## photo-processor exercise

### Description

The aim of the exercise is to orchestrate the generation of thumbnails for a specific set of photos.

### Installation

Prerequisites:  
- Docker  
- Ability to run `make`.

App is bundling Postgres and RabbitMQ instances via Docker, so please stop any local related services to avoid port conflicts. Otherwise you can amend the default port mappings on the docker-compose file.

Start the app:
- `make start`

Create or reset the db schema after booting the app:  
- `make db-schema`

Postgres PSQL can be accessed via:
- `make psql`

RabbitMQ management console can be accessed at:  
`http://localhost:15672/`  

Web app can be accessed at:  
`http://localhost:3000/`  

### Available Python libraries on the container

- Flask (for the web app)
- Psycopg2 (for accessing postgres)
- Pillow (for generating thumbnails)

You are welcome to use any Python library of choice, even replacing some of the above defaults.

### Tasks to be performed

#### 1. Update db-schema

- Add on `photo_status` enum two new statuses named `processing` and `failed`.  
- Create a table named `photo_thumbnails` with columns:
  - uuid (uuid) (auto-generated)
  - photo_uuid (uuid) (FK from photos.uuid) (not null)
  - width (smallint) (not null)
  - height (smallint) (not null)
  - url (text) (not null)
  - created_at (similar to photos.created_at)

#### 2. Add web endpoint for fetching photos of pending status

- Method: `GET`  
- URL: `/photos/pending`  

Should return photo records in JSON format.

#### 3. Add web endpoint for triggering the processing of pending photos

- Method: `POST`
- URL: `/photos/process`

Endpoint should be accepting one or more photo UUIDs as JSON input.  
It should be producing one RabbitMQ message on a queue named `photo-processor` for every photo to be processed.

#### 4. Create RabbitMQ consumer

Create a barebones RabbitMQ consumer that is listening on the `photo-proccessor` queue, processing one message at a time.   
Consumer should be running on the `waldo-app` container alongside the web app.  

#### 5. Process photo

- Update db `photos.status` to `processing`.
- Download image using `photos.url`.
- Generate a thumbnail of max 320x320 dimensions, maintaining the aspect ratio.
- Store thumbnail file on mounted `/waldo-app-thumbs` directory.
- Store a new row on db table `photo_thumbnails` with the thumbnail details. For the `photo_thumbnails.url` just use the relative path to the file.
- On success, update `photos.status` to `completed`.
- On error, update `photos.status` to `failed`.

### Deliverables

- Solution should be using Python 3.7.x, and extending the given setup.
- Git repository with any required instructions for running it, uploaded on GitHub or GitLab or BitBucket. 
