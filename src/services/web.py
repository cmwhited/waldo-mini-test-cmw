import os
import logging
import enum
import datetime
import uuid
import pika
from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Text, TIMESTAMP, Enum, SmallInteger, ForeignKey
from sqlalchemy_utils import UUIDType

# application configuration
app_config: dict = {
    'SQLALCHEMY_DATABASE_URI': os.getenv('PG_CONNECTION_URI'),
    'SQLALCHEMY_TRACK_MODIFICATIONS': False,
    'SQLALCHEMY_POOL_SIZE': 5,
    'AMQP_URI': os.getenv('AMQP_URI')
}

# instantiate logger instance
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# instantiate flask application && database connection
app = Flask(__name__)
app.config.from_mapping(app_config)
db = SQLAlchemy(app)

# instantiate amqp params
MESSAGE_CHANNEL_NAME = 'photo-processor'
amqp_uri = app.config.get('AMQP_URI')
amqp_params = pika.URLParameters(amqp_uri)
amqp_params._socket_timeout = 5


# photo status enum
class PhotoStatusEnum(enum.Enum):
    PENDING = 'pending'
    COMPLETED = 'completed'
    PROCESSING = 'processing'
    FAILED = 'failed'


# photos database model
class Photos(db.Model):
    __table_name__ = 'photos'

    uuid = Column(name='uuid', type_=UUIDType(), primary_key=True, nullable=False, default=uuid.uuid4())
    url = Column(name='url', type_=Text, nullable=False)
    status = Column(
        name='status',
        type_=Enum('pending', 'completed', 'processing', 'failed', name='photo_status', create_type=False),
        nullable=False,
        default='pending')
    created_at = Column(name='created_at', type_=TIMESTAMP, nullable=False, default=datetime.datetime.utcnow())

    def __repr__(self):
        return '<photo {}>'.format(self.uuid)

    def to_dict(self) -> dict:
        return {'uuid': self.uuid, 'url': self.url, 'status': self.status, 'created_at': self.created_at}


# photo_thumbnails database model
class PhotoThumbnails(db.Model):
    __table_name__ = 'photo_thumbnails'

    uuid = Column(name='uuid', type_=UUIDType(), primary_key=True, nullable=False, default=uuid.uuid4())
    photo_uuid = Column('photo_uuid', UUIDType(), ForeignKey('photos.uuid'), nullable=False)
    width = Column(name='width', type_=SmallInteger, nullable=False)
    height = Column(name='height', type_=SmallInteger, nullable=False)
    url = Column(name='url', type_=Text, nullable=False)
    created_at = Column(name='created_at', type_=TIMESTAMP, nullable=False, default=datetime.datetime.utcnow())

    def __init__(self, photo_uuid: uuid, width: int, height: int, url: str):
        self.photo_uuid = photo_uuid
        self.width = width
        self.height = height
        self.url = url

    def __repr__(self):
        return '<photo_thumbnail {}>'.format(self.uuid)

    def to_dict(self) -> dict:
        return {
            'uuid': self.uuid,
            'photo_uuid': self.photo_uuid,
            'width': self.width,
            'height': self.height,
            'url': self.url,
            'created_at': self.url
        }


class ProcessPhotosRequest(object):

    def __init__(self, uuids: [uuid]):
        self.uuids = uuids

    @staticmethod
    def from_request(request_data: dict):
        return ProcessPhotosRequest(request_data['uuids'])

    def to_dict(self) -> dict:
        return {'uuids': self.uuids}


def process_photos(process_photos_request: ProcessPhotosRequest) -> None:
    """
    Publish a rabbitmq message for each photo to process as uploaded in the `ProcessPhotoRequest`.
    Instantiate a `pika.BlockingConnection`, build a channel from the connection, declare a queue, iterate through each
    photo primary key id uploaded in the request and publish it as a message on the queue.
    Once finished, close the connection.
    :param process_photos_request: `ProcessPhotosRequest`, required
        the parsed received request for processing the photos. contains a list of photo primary keys to be processed
    """
    # build amqp connection and channel
    conn = pika.BlockingConnection(amqp_params)
    channel = conn.channel()
    channel.queue_declare(queue=MESSAGE_CHANNEL_NAME, durable=True)
    # publish messages
    for i in process_photos_request.uuids:
        photo_uuid = uuid.UUID(i)
        channel.basic_publish(exchange='', routing_key=MESSAGE_CHANNEL_NAME, body=photo_uuid.bytes)

    # close the connection
    conn.close()


@app.route('/')
def index():
    return jsonify(success=True)


@app.route('/photos/pending', methods=['GET'])
def get_pending_photos_handler():
    photos: [Photos] = Photos.query.filter_by(status=PhotoStatusEnum.PENDING.value).order_by(Photos.created_at).all()
    photos_json: [dict] = list(map(lambda i: i.to_dict(), photos))
    return jsonify(photos_json)


@app.route('/photos/process', methods=['POST'])
def process_photos_handler():
    request_data = request.get_json()
    process_photos_request: ProcessPhotosRequest = ProcessPhotosRequest.from_request(request_data)
    process_photos(process_photos_request)
    return jsonify(message=f'Processing {len(process_photos_request.uuids)} request')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
