import os
import logging
import pika
import uuid
import datetime
import requests
import io
import enum
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Text, TIMESTAMP, Enum, SmallInteger, ForeignKey
from sqlalchemy_utils import UUIDType
from pathlib import Path
from PIL import Image

THUMBNAIL_DIMENSIONS = 320, 320
THUMBNAIL_DIR = '/root/waldo-app-thumbs'
THUMBNAIL_FILE_TYPE = 'JPEG'
THUMBNAIL_FILE_EXT = '.jpg'
if not os.path.exists(THUMBNAIL_DIR):
    os.makedirs(THUMBNAIL_DIR)

# application configuration
app_config: dict = {
    'SQLALCHEMY_DATABASE_URI': os.getenv('PG_CONNECTION_URI'),
    'SQLALCHEMY_TRACK_MODIFICATIONS': False,
    'SQLALCHEMY_POOL_SIZE': 5,
    'AMQP_URI': os.getenv('AMQP_URI')
}

# instantiate flask application && database connection
app = Flask(__name__)
app.config.from_mapping(app_config)
db = SQLAlchemy(app)

# instantiate amqp params
MESSAGE_CHANNEL_NAME = 'photo-processor'
amqp_params = pika.URLParameters(app_config.get('AMQP_URI'))
amqp_params._socket_timeout = 5

# instantiate logger instance
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ImgFileDownloadError(Exception):
    """Unable to download file from s3"""
    pass


class ImgThumbnailError(Exception):
    """Unable to open image file to create thumbnail from"""
    pass


class NoPhotoRecordFoundError(Exception):
    pass


class ProcessPhotoError(Exception):
    pass


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


def download_img(url: str) -> io.BytesIO:
    """
    Use the requests framework to download the file from the given url.
    If the response `status_code` is not 200 (OK), thrown an `ImgFileDownloadError`.
    If the image is downloaded successfully, convert the downloaded content to a `io.BytesIO`.
    :param url: string, required
        the url where the image lives to download from
    :return: `io.BytesIO`; the downloaded image content converted to a bytes array
    :raises `ImgFileDownloadError`; raised if unable to download the file from the url
    """
    logger.info(f'download_img() - download the image from the url: {url}')
    try:
        req: requests.Response = requests.get(url)
        if req.status_code != 200 or req.content is None:
            raise ImgFileDownloadError(
                f'Unable to retrieve image content from url: {url} status code {req.status_code}')

    except (requests.HTTPError, requests.exceptions.ConnectionError) as err:
        logger.error(
            f'download_img() - http error occurred while downloading the image content from {url} with error {err}'
        )
        raise ImgFileDownloadError(err)
    else:
        return io.BytesIO(req.content)


def create_img_thumbnail(img_content: io.BytesIO, photo_uuid: uuid) -> PhotoThumbnails:
    """
    Create a thumbnail image from the downloaded image content `io.BytesIO`.
    Keep the photo aspect ratio, do not exceed the given max dimensions.
    Once the thumbnail is created, save to the mounted thumbnail directory.
    Return the url of saved image location in the mounted directory.

    Saved thumbnail url: {THUMBNAIL_DIR}/{photo_uuid}.thumbnail.{THUMBNAIL_FILE_EXT}

    Pillow Image docs for creating a thumbnail:
    https://pillow.readthedocs.io/en/3.1.x/reference/Image.html#PIL.Image.Image.thumbnail

    :param img_content: `io.BytesIO`, required
        the image content bytes
    :param photo_uuid: `uuid`, required
        the primary key id of the photo record for the photo that the thumbnail is being created for
    :return: `PhotoThumbnails`; instance built from building and saving image thumbnail
    :raises `ImgOpenError`; raised if the passed in image content cannot be opened
    """
    logger.info('create_img_thumbnail() - create the image thumbnail from the downloaded image')
    try:
        thumbnail_url = f'{THUMBNAIL_DIR}/{str(photo_uuid)}.thumbnail{THUMBNAIL_FILE_EXT}'
        thumbnail_path = Path(thumbnail_url)
        with Image.open(img_content) as img:
            # use pillow to create thumbnail of image & save to the given directory
            img.thumbnail(THUMBNAIL_DIMENSIONS, Image.ANTIALIAS)
            img.save(thumbnail_path, THUMBNAIL_FILE_TYPE)
            # to get actual thumbnail size, open the saved thumbnail
            with Image.open(thumbnail_url) as thumbnail_img:
                # get actual image dimensions
                width, height = thumbnail_img.size
    except IOError as err:
        logger.error(f'create_img_thumbnail() - unable to open or create thumbnail image with err {err}')
        raise ImgThumbnailError(err)
    else:
        return PhotoThumbnails(photo_uuid, width, height, thumbnail_url)


def init_message_consumer():
    """Init a amqp message consumer to consume any messages on our queue"""
    # build amqp connection and channel
    conn = pika.BlockingConnection(amqp_params)
    channel = conn.channel()
    channel.queue_declare(queue=MESSAGE_CHANNEL_NAME, durable=True)
    # consume messages
    for method_frame, properties, body in channel.consume(MESSAGE_CHANNEL_NAME):
        # get photo uuid PRIMARY KEY from message body & process
        photo_uuid = uuid.UUID(bytes=body)
        process_photo_message(photo_uuid)
        # acknowledge message
        channel.basic_ack(method_frame.delivery_tag)

    conn.close()


def process_photo_message(photo_uuid: uuid) -> None:
    """
    Process the photo with the given PK uuid value.
        - Retrieve the photo from the database
        - Update the photo record status to be `processing`
        - Download the img file using the requests library
        - Use the pillow lib to create a 320x320 thumbnail of the img
        - Store thumbnail to `/waldo-app-thumbs` directory
        - If successful:
            - Create a new photo_thumbnails record in the database with the thumbnail details
            - Update the status of the photo record to be `completed`
        - If failure: update the status of the photo record to be `failed`
    :param photo_uuid: uuid, required
        the PK id of the photo being processed
    :raises NoPhotoRecordFoundError; raised if no photo record can be found by the `photo_uuid` primary key
    """
    logger.info(f'process_photo() - processing photo request with id {str(photo_uuid)}')
    # retrieve the photo record
    photo: Photos = Photos.query.get(photo_uuid)
    if photo is None:
        raise NoPhotoRecordFoundError(f'No photo record found with id {str(photo_uuid)}')

    try:
        # update the photo status to processing
        photo.status = PhotoStatusEnum.PROCESSING.value
        db.session.commit()
        # download the image file
        img_content: io.BytesIO = download_img(photo.url)
        # create the thumbnail image
        photo_thumbnail: PhotoThumbnails = create_img_thumbnail(img_content, photo_uuid)
        db.session.add(photo_thumbnail)
        db.session.commit()
        # update photo status to completed
        photo.status = PhotoStatusEnum.COMPLETED.value
        db.session.commit()
    except (ImgFileDownloadError, ImgThumbnailError) as ex:
        logger.error(f'process_photo() - an error occurred processing photo with id {photo_uuid} with error {ex}')
        # update photo status to failed
        photo.status = PhotoStatusEnum.FAILED.value
        db.session.commit()
        # raise process photo error
        raise ProcessPhotoError(ex)


if __name__ == '__main__':
    init_message_consumer()
    # start app
    app.run(host='0.0.0.0', port=3001)
