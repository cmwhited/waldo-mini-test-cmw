import os
import logging
import enum
import datetime
import uuid
from flask import Flask, jsonify
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

# instantiate flask application
app = Flask(__name__)
app.config.from_mapping(app_config)
db = SQLAlchemy(app)


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


@app.route('/')
def index():
    return jsonify(success=True)


@app.route('/photos/pending', methods=['GET'])
def get_pending_photos():
    photos: [Photos] = Photos.query.filter_by(status=PhotoStatusEnum.PENDING.value).order_by(Photos.created_at).all()
    photos_json: [dict] = list(map(lambda i: i.to_dict(), photos))
    return jsonify(photos_json)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
