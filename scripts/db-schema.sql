CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS photos;
DROP TYPE IF EXISTS photo_status;

CREATE TYPE photo_status as enum('pending', 'completed');
CREATE TABLE photos (
    uuid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    url text NOT NULL,
    status photo_status DEFAULT 'pending' NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

INSERT INTO photos (url)
VALUES
  ('https://s3.amazonaws.com/waldo-thumbs-dev/large/71840919-e422-552d-8c8d-9b2b360ce98c.jpg'),
  ('https://s3.amazonaws.com/waldo-thumbs-dev/large/72800f95-c406-5475-85ac-b8943877b15f.jpg'),
  ('https://s3.amazonaws.com/waldo-thumbs-dev/large/366ad885-aafd-48a4-8ff5-c38a1bbc84c8.jpg'),
  ('https://s3.amazonaws.com/waldo-thumbs-dev/large/b3cbaef4-ff6d-523e-beea-704629c42ca2.jpg'),
  ('https://s3.amazonaws.com/waldo-thumbs-dev/large/49dd12b2-f019-59c5-bf17-b1b3bb208eba.jpg'),
  ('https://s3.amazonaws.com/waldo-thumbs-dev/large/c9391205-1892-5139-abe5-b5df3ced8d61.jpg');
